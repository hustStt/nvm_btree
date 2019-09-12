#pragma once

#include <cstring>
#include <string>
#include <climits>
#include <future>
#include <mutex>
#include <atomic>

#include "nvm_common.h"
#include "random.h"

using namespace rocksdb;

const int SkipMaxHeight = 20;

struct SkipNode {
    uint64_t key;
    void *value;
    std::atomic<SkipNode*> next_[1];

    SkipNode(const uint64_t &k, void *v = nullptr) {
        key = k;
        value = v;
    }

    ~SkipNode() {

    }

    SkipNode* Next(int n) {
        assert(n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned SkipNode.
        return next_[n].load(std::memory_order_acquire);
    }
    void SetNext(int n, SkipNode* x, bool persist = false) {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted node.
        next_[n].store(x, std::memory_order_release);
        if(persist) {
            nvm_persist(&next_[n], sizeof(std::atomic<SkipNode*> ));
        }
    }

    // No-barrier variants that can be safely used in a few locations.
    SkipNode* NoBarrier_Next(int n) {
        assert(n >= 0);
        return next_[n].load(std::memory_order_relaxed);
    }

    void NoBarrier_SetNext(int n, SkipNode* x, bool persist = false) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);

        if(persist) {
            nvm_persist(&next_[n], sizeof(std::atomic<SkipNode*>));
        }
    }
};

class SkipList {
private:
    SkipNode *head_;
    uint16_t kBranching_;
    // uint32_t kScaledInverseBranching_;
    std::atomic<uint16_t> max_height_;
    // size_t key_size_;
    Random rnd_;

    Statistic stats;
public:
// explicit SkipList(PersistentAllocator* allocator, int32_t max_height = 12, int32_t branching_factor = 4, size_t key_size = 16 ,uint64_t opt_num = 0, size_t per_1g_num = 0);
    explicit SkipList(uint16_t branching_factor = 4)
        : rnd_(0xdeadbeef) {
        kBranching_ = branching_factor;
        head_ = NewNode(0, SkipMaxHeight);
        max_height_ = 1;
        for(int i =0; i < SkipMaxHeight; i ++) {
            head_->SetNext(i, nullptr);
        }
        nvm_persist(head_, sizeof(SkipNode) + SkipMaxHeight * sizeof(std::atomic<SkipNode *>));
    }
    ~SkipList() {
#ifdef CAL_ACCESS_COUNT
        // PrintAccessTime();
#endif
    }

    void Insert(const uint64_t &key, void *value) {
        SkipNode *prev[SkipMaxHeight];

        SkipNode *x = FindGreaterOrEqual(key, prev);

        // print_log(LV_DEBUG, "Call ");

        int height = RandomHeight();

        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; i++) {
                prev[i] = head_;
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            max_height_.store(height, std::memory_order_relaxed);
        }

        x = NewNode(key, height, value);

        for(int i = 0; i < height; i ++) {
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, x, true);
        }

        nvm_persist(x, sizeof(SkipNode) + height * (sizeof(std::atomic<SkipNode *>)));
    }

    bool Update(const uint64_t &key, void *value) {
        SkipNode *x = FindGreaterOrEqual(key);

        if(x != nullptr && key == x->key) {
            x->value = value;
            nvm_persist(&x->value, sizeof(void *));
            return true;
        }
        return false;
    }

    void *Get(const uint64_t key) {
        SkipNode *x = FindGreaterOrEqual(key);

        if(x != nullptr && key == x->key) {
            return x->value;
        }
        return nullptr;
    }

    bool Delete(const uint64_t &key) {
        SkipNode *prev_[SkipMaxHeight];
        SkipNode *x = FindLessThan(key, prev_); 

        if(x == nullptr) {
            return false;
        }

        SkipNode *current_node = x->Next(0);

        if(current_node->key != key) {
            return false;
        }
        for(int i=0; i < GetMaxHeight(); i ++) {
            if(prev_[i] != nullptr && prev_[i]->NoBarrier_Next(i) == current_node) {
                prev_[i]->SetNext(i, current_node->NoBarrier_Next(i), true);
            } else {
                break;
            }
        }
        return true;
    }

    void GetRange(uint64_t &key1, uint64_t &key2, std::vector<std::string> &values, int &size) {
        int find_size = 0;

        SkipNode *x = FindGreaterOrEqual(key1);
        while(x != nullptr) {
            if(x->key > key2) {
                size = find_size;
                return ;
            }
            values.push_back(string((char *)x->value, NVM_ValueSize));
            find_size ++;
            if(find_size >= size) {
                return ;
            }
            x = x->Next(0);
        }
        size = find_size;
    }

    void Print() const {
        for(int i=0; i < GetMaxHeight(); i++){
            SkipNode* start = head_->Next(i);
            int num=0;
            printf("Level %d \n", i);
            while(start != nullptr){
                uint64_t value_point;
                printf("\t%010d %llx %llx\n", num, start->key, start->value);
                num++;
                start=start->Next(i);
            }
        }
        printf("\n");
    }

    void PrintLevelNum() const {
        for(int i=0; i < GetMaxHeight(); i++){
            SkipNode* start = head_->Next(i);
            int num=0;
            while(start != nullptr){
                num++;
                start=start->Next(i);
            }
            if(num > 0)
                printf("Level %d = %d\n",i,num);
        }
        printf("\n");
    }

    void PrintInfo() const {
        printf("NVM skip list max height = %d\n", SkipMaxHeight);
        printf("Skip list max height = %d of %d\n", GetMaxHeight(), SkipMaxHeight);
        printf("Skip Node size = %d\n", sizeof(SkipNode));
    }

    void PrintStatistic() {

    }


#ifdef CAL_ACCESS_COUNT
    void PrintAccessTime() {

    }
#endif

private:
    inline int GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }

    SkipNode* NewNode(const uint64_t key, int height, void *value = nullptr) {
        assert(height <= SkipMaxHeight);
        char* mem = node_alloc->Allocate(sizeof(SkipNode) + (height - 1) * (sizeof(SkipNode *)), 8);
        return new (mem) SkipNode(key, value);
    }

    int RandomHeight() {
        int height = 1;
        // while (height < SkipMaxHeight && rnd_->Next() < kBranching_) {
        int rand = 0;
        while (height < SkipMaxHeight && (((rand =rnd_.Next()) % kBranching_) == 0)) {    // level DB
            height++;
        }
        // print_log(LV_DEBUG, "height is %d, rand is %d, factor is %d", height, rand, kBranching_);
        return height;
    }

    bool Equal(const char *a, const char *b) {
        return strcmp(a, b) == 0;
    }

    bool LessThan(const char *a, const char *b) {
        return strcmp(a, b) < 0;
    }

    bool KeyIsAfterNode(const uint64_t &key, SkipNode* n) const {
        return (n != nullptr && key > n->key);
    }
    
    int CompareKeyAndNode(const uint64_t &key, SkipNode* n) const {
        if(key > n->key) {
            return 1;
        } else if(key < n->key) {
            return -1;
        }
        return 0;
    }

    SkipNode* FindGreaterOrEqual(const uint64_t &key,  SkipNode** prev = nullptr) const {
        SkipNode* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            SkipNode* next = x->Next(level);
            if (KeyIsAfterNode(key, next)) {
                // Keep searching in this list
                x = next;
            } else {
                if (prev != nullptr) prev[level] = x;
                if (level == 0) {
                    return next;
                } else {
                    // Switch to next list
                    level--;
                }
            }
        }
    }

    SkipNode *FindLessThan(const uint64_t &key, SkipNode** prev = nullptr) const {
        SkipNode* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            assert(x == head_ || x->key < key);
            SkipNode* next = x->Next(level);
            if (next == nullptr || next->key >= key) {
                if (prev != nullptr) prev[level] = x;
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }
};
