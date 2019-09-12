#pragma once

#include <cstring>
#include <string>
#include <climits>
#include <future>
#include <mutex>

#include "nvm_common.h"
#include "random.h"

using namespace rocksdb;

const int SkipList_NodeSize = 256;
const int NVM_SkipMaxHeight = (SkipList_NodeSize - NVM_KeyBuf) / NVM_PointSize;

struct NVMSkipNode {
    explicit NVMSkipNode(string key) {
        nvm_memcpy_persist(key_, key.c_str(), key.size());
    };
    ~NVMSkipNode() = default;
    
    NVMSkipNode* Next(int n) {
        assert(n >= 0);
        return next_[n];
    }

    void SetNext(int n, NVMSkipNode* next) {
        assert(n >= 0);
        nvm_memcpy_persist(next_ + n, &next, sizeof(NVMSkipNode*));
    }
    char key_[NVM_KeyBuf];
    NVMSkipNode *next_[];
};

class SkipList {
    public:
// explicit SkipList(PersistentAllocator* allocator, int32_t max_height = 12, int32_t branching_factor = 4, size_t key_size = 16 ,uint64_t opt_num = 0, size_t per_1g_num = 0);
        explicit SkipList(int32_t branching_factor = 4);
        ~SkipList() {
#ifdef CAL_ACCESS_COUNT
            // PrintAccessTime();
#endif
        }

        void Insert(const std::string& key);

        string Get(const std::string &param_key);

        void Delete(const std::string& key);

        void Insert(const std::string& key, const std::string& value);

        void PrintKey(const char *str,uint64_t &last_num, uint64_t &last_seq_num) const;

        void GetRange(const std::string& key1, const std::string& key2, std::vector<std::string> &values, int &size);

        void Print() const;

        void PrintLevelNum() const;

        void PrintInfo();

        void PrintStatistic();

        string GetValue(char *valuepointer);

#ifdef CAL_ACCESS_COUNT
        void PrintAccessTime();
#endif

    private:
        NVMSkipNode *head_;
        NVMSkipNode *prev_[NVM_SkipMaxHeight];
        uint32_t prev_height_;
        uint16_t kMaxHeight_;
        uint16_t kBranching_;
        uint32_t kScaledInverseBranching_;
        uint16_t max_height_;
        // size_t key_size_;
        Random rnd_;

        Statistic stats;

        inline int GetMaxHeight() const {
            return max_height_;
        }

        NVMSkipNode* NewNode(const std::string &key, int height);

        int RandomHeight();

        bool Equal(const char *a, const char *b) {
            return strcmp(a, b) == 0;
        }

        bool LessThan(const char *a, const char *b) {
            return strcmp(a, b) < 0;
        }

        bool KeyIsAfterNode(const std::string& key, NVMSkipNode* n) const;
        
        int CompareKeyAndNode(const std::string& key, NVMSkipNode* n);

        NVMSkipNode* FindGreaterOrEqual(const std::string& key) const;

        void FindLessThan(const std::string& key, NVMSkipNode** prev = nullptr);

        void FindNextNode(const std::string &key, NVMSkipNode** prev);

        char *skipListDynamicQuery(const std::string &key);

        char *skipListStaticQuery(const std::string &key);

        void skipListDynamicDelete(const std::string& key);
    };