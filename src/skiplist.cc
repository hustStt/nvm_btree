#include "skiplist.h"

#ifdef CAL_ACCESS_COUNT
        uint64_t all_cnt_  = 0;
        uint64_t head_cnt_ = 0;
        uint64_t suit_cnt_ = 0;
#endif

    SkipList::SkipList(int32_t branching_factor)
        : rnd_(0xdeadbeef) {
        assert(SkipList_NodeSize >= sizeof(NVMSkipNode));
        kBranching_ = static_cast<uint16_t>(branching_factor),
        kScaledInverseBranching_ = (Random::kMaxNext + 1) / kBranching_;
        max_height_ = 1;
        kMaxHeight_ = NVM_SkipMaxHeight;

        assert(SkipList_NodeSize >= (sizeof(NVMSkipNode*) * kMaxHeight_));
        head_ = NewNode(" ", kMaxHeight_);
        for (int i = 0; i < kMaxHeight_; i++) {
            head_->SetNext(i, nullptr);
            prev_[i] = head_;
            // pmem_flush(prev_ + i, sizeof(NVMSkipNode*));
        }
        // pmem_drain();
        prev_height_ = 1;
        stats.clear_period();
    }

    NVMSkipNode* SkipList::NewNode(const std::string &key, int height) {
        assert(height <= NVM_SkipMaxHeight);
        char* mem = node_alloc->Allocate(sizeof(NVMSkipNode) + height * (sizeof(NVMSkipNode *)));
        return new (mem) NVMSkipNode(key);
    }

    int SkipList::RandomHeight() {
        int height = 1;
        // while (height < kMaxHeight_ && rnd_->Next() < kScaledInverseBranching_) {
        while (height < kMaxHeight_ && ((rnd_.Next() % kBranching_) == 0)) {    // level DB
            height++;
        }
        return height;
    }

    bool SkipList::KeyIsAfterNode(const std::string& key, NVMSkipNode* n) const {
        return (n != nullptr) && (memcmp(n->key_, key.c_str(), NVM_KeySize) < 0);
    }

    int SkipList::CompareKeyAndNode(const std::string& key, NVMSkipNode* n) {
        int res = memcmp(key.c_str(), n->key_, NVM_KeySize);        // 16 equal KEY_SIZE
        return res;
    }

    // ignore function name
    void SkipList::FindLessThan(const std::string &key, NVMSkipNode** prev) {
        NVMSkipNode* x = head_;
        int level = GetMaxHeight() - 1;
#ifdef CAL_ACCESS_COUNT
        all_cnt_ += 1;
        head_cnt_ += 1;
#endif

        for (int i = level; i >= 0; i--) {
            NVMSkipNode *next = x->Next(i);
            while (KeyIsAfterNode(key, next)) {
                x = next;
                next = x->Next(i);
                stats.add_node_search();
            }
            prev[i] = x;
        }
    }

    void SkipList::FindNextNode(const std::string &key, NVMSkipNode** prev)
    {
        // 从prev[level]节点往后查找合适的node
        int level = GetMaxHeight() - 1;
        NVMSkipNode *x = prev[level];

        for (int i = level; i >= 0; i--) {
            NVMSkipNode* next = x->Next(i);
            while (KeyIsAfterNode(key, next)) {
                x = next;
                next = x->Next(i);
                stats.add_node_search();
            }
            prev[i] = x;
        }
    }

    void SkipList::Insert(const std::string &key) {
        // key < prev[0]->next(0) && prev[0] is head or key > prev[0]
        //printf("prev[0] is %s\n", prev_[0]== nullptr?"null":"not null");
        stats.add_tree_level(GetMaxHeight());
        int res = CompareKeyAndNode(key, prev_[0]);
        if (res > 0) {
            // key is after prev_[0]
            if (prev_[0]->Next(0) == nullptr) {
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                ;
            } else {
                int res_next = CompareKeyAndNode(key, prev_[0]->Next(0));
                if (res_next < 0) {     // 节点插入在prev_[0] 与 prev_[0]->Next(0)之间
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                    ;
                } else if (res_next > 0) {
                    FindNextNode(key, prev_);
                } else {
                    printf("impossible key is equal!\n");
                }
            }
            stats.add_node_search();
        } else if (res < 0) {
            // 从头开始遍历查找
            FindLessThan(key, prev_);
        } else {
            // 直接覆盖数据，不需要新建节点
            ; // impossible
            printf("key is equal\n");
        }

        int height = RandomHeight();
        if(height > GetMaxHeight()){
            for(int i = GetMaxHeight(); i < height; i++){
                prev_[i] = head_;
            }
            max_height_ = static_cast<uint16_t>(height);
        }

        NVMSkipNode* x = NewNode(key, height);
        for(int i = 0; i < height; i++){
            x->SetNext(i, prev_[i]->Next(i));
            prev_[i]->SetNext(i ,x);
            prev_[i] = x;
            // pmem_persist(prev_[i], sizeof(NVMSkipNode*));
        }
        //prev_[0] = x;
        //pmem_persist(prev_[0], sizeof(NVMSkipNode*));
        prev_height_ = static_cast<uint16_t >(height);
        stats.add_entries_num();
    }

    void SkipList::Insert(const std::string &param_key, const std::string &value) {
        stats.add_tree_level(GetMaxHeight());
        char keybuf[NVM_KeyBuf + 1];
        stats.start();
        char *pvalue = value_alloc->Allocate(value.size());
        uint64_t vpoint = (uint64_t)pvalue;
        pmem_memcpy_persist(pvalue, value.c_str(), value.size());
        memcpy(keybuf, param_key.c_str(), param_key.size());
        memcpy(keybuf + NVM_KeySize, &vpoint, NVM_PointSize);
        string key(keybuf, NVM_KeyBuf);

        int res = CompareKeyAndNode(key, prev_[0]);
        if (res > 0) {
            // key is after prev_[0]
            if (prev_[0]->Next(0) == nullptr) {
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                ;
            } else {
                int res_next = CompareKeyAndNode(key, prev_[0]->Next(0));
                if (res_next < 0) {     // 节点插入在prev_[0] 与 prev_[0]->Next(0)之间
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                    ;
                } else if (res_next > 0) {
                    FindNextNode(key, prev_);
                } else {
                    printf("impossible key is equal!\n");
                }
            }
            stats.add_node_search();
        } else if (res < 0) {
            // 从头开始遍历查找
            FindLessThan(key, prev_);
        } else {
            printf("key is equal\n");
        }

        int height = RandomHeight();
        if(height > GetMaxHeight()){
            for(int i = GetMaxHeight(); i < height; i++){
                prev_[i] = head_;
            }
            max_height_ = static_cast<uint16_t>(height);
        }

        NVMSkipNode* x = NewNode(key, height);
        for(int i = 0; i < height; i++){
            x->SetNext(i, prev_[i]->Next(i));
            prev_[i]->SetNext(i ,x);
            prev_[i] = x;
            // pmem_persist(prev_[i], sizeof(NVMSkipNode*));
        }
        //prev_[0] = x;
        //pmem_persist(prev_[0], sizeof(NVMSkipNode*));
        prev_height_ = static_cast<uint16_t >(height);
        stats.add_entries_num();
    }

    char *SkipList::skipListDynamicQuery(const std::string &key) {
        stats.add_tree_level(GetMaxHeight());
        // 首先比较当前位置的数据
        int res = CompareKeyAndNode(key, prev_[0]);
        if (res > 0) {
            // key is after prev_[0]
            if (prev_[0]->Next(0) == nullptr) {
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                return nullptr;
            } else {
                int res_next = CompareKeyAndNode(key, prev_[0]->Next(0));
                if (res_next < 0) {     // 节点插入在prev_[0] 与 prev_[0]->Next(0)之间
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                    return nullptr; // Not find
                } else if (res_next > 0) {
                    FindNextNode(key, prev_);
                    if(prev_[0]->Next(0) != nullptr && CompareKeyAndNode(key, prev_[0]->Next(0)) == 0) { // find the key 
                        return prev_[0]->Next(0)->key_ + NVM_KeySize;
                    }
                } else {
                    // printf("impossible key is equal!\n");
                    return prev_[0]->Next(0)->key_ + NVM_KeySize;
                }
            }
            stats.add_node_search();
        } else if (res < 0) {
            // 从头开始遍历查找
            FindLessThan(key, prev_);
            if(prev_[0]->Next(0) != nullptr && CompareKeyAndNode(key, prev_[0]->Next(0)) == 0) { // find the key 
                return prev_[0]->Next(0)->key_ + NVM_KeySize;
            }

        } else {
           return prev_[0]->key_ + NVM_KeySize;
        }
        return nullptr;
    }

    string SkipList::Get(const std::string &param_key) {
        char *pvalue;
        stats.start();
        if((pvalue = skipListDynamicQuery(param_key)) == nullptr) {
            return "";
        } else {
            uint64_t value_point;
            memcpy(&value_point, pvalue, sizeof(uint64_t));
            char *value = (char *)value_point;
            return string(value, NVM_ValueSize); 
        }
    }


    void SkipList::skipListDynamicDelete(const std::string& key) {
        stats.add_tree_level(GetMaxHeight());
        // 首先比较当前位置的数据
        int res = CompareKeyAndNode(key, prev_[0]);
        bool find = false;
        if (res > 0) {
            // key is after prev_[0]
            if (prev_[0]->Next(0) == nullptr) {  //当前是最后一个节点
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                return ;
            } else {
                int res_next = CompareKeyAndNode(key, prev_[0]->Next(0));
                if (res_next < 0) {     // 节点在prev_[0] 与 prev_[0]->Next(0)之间
#ifdef CAL_ACCESS_COUNT
                suit_cnt_ += 1;
#endif
                    return ; // Not find
                } else if (res_next > 0) {
                    FindNextNode(key, prev_);
                    if(prev_[0]->Next(0) != nullptr && CompareKeyAndNode(key, prev_[0]->Next(0)) == 0) {
                        find = true;
                    }
                } else {
                    find = true;
                }
            }
            stats.add_node_search();
        } else {
            // 从头开始遍历查找
            FindLessThan(key, prev_);
            if(prev_[0]->Next(0) != nullptr && CompareKeyAndNode(key, prev_[0]->Next(0)) == 0) {
                 find = true;       
            }
        }

        if(find) { // find the key 
            int maxheight = GetMaxHeight();
            struct NVMSkipNode *current_node = prev_[0]->Next(0);
            for(int i = 0; i < maxheight; i ++) {
                if(prev_[i] != nullptr && prev_[i]->Next(i) == current_node) {
                    prev_[i]->SetNext(i, prev_[i]->Next(i)->Next(i));
                } else {
                    // i --;
                    // while(i > 0 && prev_[i] == head_ && prev_[i]->Next(i) == nullptr) {      // 重新设置最大高度
                    //     i --;
                    // }
                    // if(i < max_height_) {
                    //     max_height_ = static_cast<uint16_t>(i);
                    // }
                    return ;
                }
            }
        }

    }

    void SkipList::Delete(const std::string& key) {
        skipListDynamicDelete(key);
        // FindLessThan(key, prev_);
        // if(prev_[0]->Next(0) != nullptr && CompareKeyAndNode(key, prev_[0]->Next(0)) == 0) {
        //     int maxheight = GetMaxHeight();
        //     struct NVMSkipNode *current_node = prev_[0]->Next(0);
        //     for(int i = 0; i < max_height_; i ++) {
        //         if(prev_[i] != nullptr && prev_[i]->Next(i) == current_node) {
        //             prev_[i]->SetNext(i, prev_[i]->Next(i)->Next(i));
        //         } else {
        //             break ;
        //         }
        //     }     
        // }
    }

    NVMSkipNode* SkipList::FindGreaterOrEqual(const std::string &key) const {
        NVMSkipNode* x = head_;
        int level = GetMaxHeight() - 1;
        NVMSkipNode* last_bigger;
        while(true){
            NVMSkipNode* next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : strcmp(next->key_, key.c_str());
            if(cmp == 0 || (cmp > 0 && level ==0)){
                return next;
            } else if(cmp < 0) {
                x = next;
            } else {
                last_bigger = next;
                level--;
            }
        }
    }

    void SkipList::PrintKey(const char *str ,uint64_t &last_num, uint64_t &last_seq_num) const {
        // uint64_t key_num, seq_num;
        // key_num = 0;    seq_num = 0;

        // for (int i = 0; i < 8; i++) {
        //     key_num = key_num * 10 + (str[i] - '0');
        // }
        // for (int i = 8; i < 16; i++) {
        //     seq_num = seq_num * 10 + (str[i] - '0');
        // }
        // printf("%llu - %llu    ", key_num, seq_num);
        uint64_t num = 0;
        uint64_t seq_num = 0;
        for (int i = 0; i < 8; i++) {
            num = (str[i] - '0') + 10 * num;
        }
        
        for (int i = 8; i < 16; i++) {
            seq_num = (str[i] - '0') + 10 * seq_num;
        }

        bool res = ((last_num < num) || ((last_num == num) && (last_seq_num < seq_num)));
        if (res == false) {
            printf("----------------error: DRAM skiplist is not in order!!! -----------------------------\n\n\n\n");
        }

        last_num = num;
        last_seq_num = seq_num;
    }

    void SkipList::Print() const {
        // NVMSkipNode* start = head_->Next(0);
        // uint64_t last_num = 0;
        // uint64_t last_seq_num = 0;
        // while(start != nullptr) {
        //     PrintKey(start->key_,last_num, last_seq_num);
        //     start = start->Next(0);
        // }
        for(int i=0;i < GetMaxHeight(); i++){
            NVMSkipNode* start = head_->Next(i);
            int num=0;
            printf("Level %d \n", i);
            while(start != nullptr){
                uint64_t value_point;
                memcpy(&value_point, start->key_ + NVM_KeySize, sizeof(uint64_t));
                printf("\t%010d %s %llx\n", num, start->key_, value_point);
                num++;
                start=start->Next(i);
            }
        }
        printf("\n");
    }

    string SkipList::GetValue(char *valuepointer) {
        uint64_t value_point;
        memcpy(&value_point, valuepointer, sizeof(uint64_t));
        char *value = (char *)value_point;
        return string(value, NVM_ValueSize); 
    }

    void SkipList::GetRange(const std::string& key1, const std::string& key2, std::vector<std::string> &values, int &size) {
        int findsize = 0;
        skipListDynamicQuery(key1);
        if(prev_[0] == nullptr) {
            size = 0;
            return;
        }
        if(CompareKeyAndNode(key1, prev_[0]) == 0) {
            values.push_back(GetValue(prev_[0]->key_ + NVM_KeySize));
            findsize ++;
        }
        NVMSkipNode* start = prev_[0]->Next(0);
        while(start != nullptr && findsize < size) {
            if(key2.size() != 0 && CompareKeyAndNode(key2, start) < 0) {
                break;
            }
            // string value = GetValue(start->key_ + NVM_KeySize);
            values.push_back(GetValue(start->key_ + NVM_KeySize));
            findsize ++;
            start = start->Next(0);
        }
        size = findsize;
    }

    void SkipList::PrintLevelNum() const {
        for(int i=0; i < GetMaxHeight(); i++){
            NVMSkipNode* start = head_->Next(i);
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

    void SkipList::PrintInfo() {
        printf("NVM skip list max height = %d\n", NVM_SkipMaxHeight);
        printf("Skip list max height = %d of %d\n", GetMaxHeight(), kMaxHeight_);
        printf("Skip Node size = %d\n", sizeof(NVMSkipNode));
    }

    void SkipList::PrintStatistic() {
        // stats.print_statinfo();
        stats.clear_period();
    }
