
#pragma once 

#include <climits>
#include <future>
#include <mutex>
#include <iostream>
#include <algorithm>
#include <map>

#include "nvm_common.h"

enum OpFlag {
    OpInsert = 0,
    OpUpdate,
    OpDelete,
};

struct Element {
    uint8_t flag;
    uint64_t key;
    void *value;

    uint8_t GetFlag(){
        return flag;
    }
    void SetFlag(uint8_t f){
        flag = f;
    }
    uint64_t GetKey() {
        return key;
    }
    void SetKey(uint64_t &k) {
        key = k;
    }
};

const int NV_NodeSize = 256;
const int NTMAX_WAY = (NV_NodeSize - sizeof(void *) - 2 - sizeof(std::mutex)) / (sizeof(uint64_t) + sizeof(void *));
const int IndexWay = (NV_NodeSize - 2) / sizeof(uint64_t);
const int LeafMaxEntry = (NV_NodeSize - sizeof(void *) - 2) / sizeof(Element);

class PLeafNode;
class IndexNode;

class LeafNode {
public:
    Element elements[LeafMaxEntry];
    LeafNode* next;
    int16_t nElements;
public:
    LeafNode() {
        nElements = 0;
        next = nullptr;
    }

    ~LeafNode() {

    }

    bool IsFull(){
        return nElements == LeafMaxEntry;
    }

    uint64_t Get_MaxKey() {
        uint64_t max_key = 0;
        for(int j = 0; j < nElements; j ++) {
            if(max_key == 0) {
                max_key = elements[j].key;
            }
            else if(max_key < elements[j].key) {
                max_key = elements[j].key;
            }
        }
        return max_key;
    }

    void Print() {
        uint64_t max_key = 0;
        for(int j = 0; j < nElements; j ++) {
            print_log(LV_INFO, "key: %16llx, value %p flag %d.", elements[j].key, 
                elements[j].value, elements[j].flag);
        }
    }

    // int DoSth(LeafNode* valid, bool has_right); //1:split,2:replace,3:merge
    // void Split(LeafNode* valid, LeafNode* left, LeafNode* right);
    // void Merge(LeafNode* valid);
} __attribute__((aligned(64)));


class PLeafNode {
public:
    int16_t n_keys;
    uint64_t m_key[NTMAX_WAY];
    LeafNode *LNs[NTMAX_WAY + 1];
    std::mutex mut;

public:
    PLeafNode() {
        n_keys = 0;
    }

    ~PLeafNode() {

    }
    
    int binary_search(uint64_t key) {
        int l = 0, r = n_keys - 1;
        while (l < r)
        {
            int mid = (l + r) / 2;
            if (m_key[mid] >= key)
            {
                r = mid;
            }
            else
            {
                l = mid + 1;
            }
        }
        return l;
    }

    uint64_t GetKey(int off) {
        return m_key[off];
    }

    uint64_t Get_MaxKey() {
        if(n_keys == 0) {
            return (uint64_t) -1;
        }
        return m_key[n_keys -1];
    } 

    bool insert(uint64_t key, LeafNode *child) {
        int d = binary_search(key);

        if (m_key[d] == key)
        {
            assert(d >= n_keys - 1);
        }
        for (int i = n_keys; i > d; i--)
        {
            m_key[i] = m_key[i - 1];
            LNs[i+1] = LNs[i];
        }
        m_key[d] = key;
        LNs[d + 1] = child;
        assert(d + 1 < NTMAX_WAY);
        n_keys ++;

        if(n_keys == NTMAX_WAY) {
            return true;
        }
        // nvm_persist(this, sizeof(PLeafNode));
        return false;
    }

    void CheckNode() {
        for(int i = 0; i < n_keys -1; i++) {
            if(m_key[i] > m_key[i + 1]) {
                print_log(LV_DEBUG, "Unexcept key greater than next key.");
                Print();
                assert(0);
            }
        }
    }

    void Print() {
        for(int i = 0; i < n_keys; i ++) {
            print_log(LV_INFO, "pnode key: %16llx, leaf node %p", m_key[i], LNs[i]);
            if(LNs[i]){
                LNs[i]->Print();
            }
        }
    }

} __attribute__((aligned(64)));


class IndexNode {
public:
    int16_t n_keys;
    uint64_t m_key[IndexWay];

public:
    int binary_search(uint64_t key) {
        int l = 0, r = n_keys - 1;
        while (l < r)
        {
            int mid = (l + r) / 2;
            if (m_key[mid] >= key)
            {
                r = mid;
            }
            else
            {
                l = mid + 1;
            }
        }
        return l;
    }

    uint64_t GetKey(int off) {
        return m_key[off];
    }

    uint64_t Get_MaxKey() {
        if(n_keys == 0) {
            return (uint64_t) -1;
        }
        return m_key[n_keys -1];
    } 

    void CheckKey(int off, uint64_t key) {
       if(off > 0 && m_key[off-1] > key) {
           print_log(LV_ERR, "Key %16llx less than before %16llx.", key, m_key[off-1]);
       }

        if(m_key[off] < key) {
           print_log(LV_ERR, "Key %16llx greater than next %llx.", key, m_key[off]);
       }
    }

    void Print() {
        for(int i = 0; i < n_keys; i ++) {
            print_log(LV_INFO, "Index node key: %16llx", m_key[i]);
        }
    }

} __attribute__((aligned(64)));

class NVTree {
    IndexNode *iNode;
    PLeafNode *pNode;
    int MaxIndex;
    int indexlevel = 0;
    int pCount;
    int lCount;
public:

    friend class LeafNode;
    friend class PLeafNode;
    friend class IndexNode;

    NVTree() {
        MaxIndex = 0;
        pCount = 1;
        lCount = 1;
        iNode = nullptr;
        pNode = new (node_alloc->Allocate(sizeof(PLeafNode))) PLeafNode();
        LeafNode *leaf = new (node_alloc->Allocate(sizeof(LeafNode))) LeafNode();
        pNode->m_key[0] = (uint64_t)-1;
        pNode->LNs[0] = leaf;
        pNode->n_keys = 1;
    }

    ~NVTree() {

    }

    PLeafNode *find_pnode(uint64_t key, int &id) {
        int pos = 0;
        id = 0;
        while(id < MaxIndex) {
            pos = iNode[id].binary_search(key);
            // iNode[id].CheckKey(pos, key);
            id = id * IndexWay + 1 + pos;
        }
        id -= MaxIndex; 
        assert(id < pCount);
        return (pNode + id);

    }

    void find_leaf(uint64_t &key, int &id, int &pos, PLeafNode *&parent, LeafNode *&leaf) {
        parent = find_pnode(key, id);

        pos = parent->binary_search(key);
        leaf = parent->LNs[pos];
    }

    void rebuild() {
        int level = 0;
        int tmp_count = lCount;
        print_log(LV_INFO, "Start");
        chrono::high_resolution_clock::time_point start_ = chrono::high_resolution_clock::now();
        while(tmp_count > 0) {
            level ++;
            tmp_count /= IndexWay;
        }

        int index_start[level];
        int index_count[level];
        index_start[0] = 0;
        index_count[0] = 1;
        for(int j = 1; j < level; j ++) {
            index_start[j] = index_start[j-1] + index_count[j-1];
            index_count[j] = index_count[j-1] * IndexWay;
        }
        PLeafNode *interim_pNode = (PLeafNode *)node_alloc->Allocate(sizeof(PLeafNode) * lCount);
        int interim_pCount = 0;
        for(int j = 0; j < pCount; j ++) {
            PLeafNode *tmp_pNode = pNode + j;
            for(int k = 0; k < tmp_pNode->n_keys; k ++) {
                PLeafNode *p = new (interim_pNode + interim_pCount) PLeafNode();
                p->m_key[0] = tmp_pNode->m_key[k];
                p->LNs[0] = tmp_pNode->LNs[k];
                p->n_keys = 1;
                interim_pCount ++;
            }
        }
        assert(interim_pCount == lCount);
        int interim_MaxIndex = index_start[level-1] + index_count[level-1];
        IndexNode *interim_iNode = (IndexNode *)node_alloc->Allocate(sizeof(IndexNode) * interim_MaxIndex);

        {
            int i = level - 1;
            for(int j = 0; j < index_count[i]; j ++) {
                int id = index_start[i] + j;
                IndexNode *tmp_iNode =  interim_iNode + index_start[i] + j;
                tmp_iNode->n_keys = 0;
                for(int k = 0; k < IndexWay; k++) {
                    int pindex = id * IndexWay + k + 1 - interim_MaxIndex;
                    if(pindex >= interim_pCount) {
                        pindex = interim_pCount -1;
                    }
                    PLeafNode *p = interim_pNode + pindex;
                    tmp_iNode->m_key[k] = p->Get_MaxKey();
                }
                tmp_iNode->n_keys = IndexWay;
            }
        }
        

        for(int i = level - 2; i >= 0; i --) {
            for(int j = 0; j < index_count[i]; j ++) {
                int id = index_start[i] + j;
                IndexNode *tmp_iNode =  interim_iNode + index_start[i] + j;
                tmp_iNode->n_keys = 0;
                for(int k = 0; k < IndexWay; k++) {
                    IndexNode *p = interim_iNode + (id * IndexWay + k + 1);
                    tmp_iNode->m_key[k] = p->Get_MaxKey();
                }
                tmp_iNode->n_keys = IndexWay;
            }
        }
        
        MaxIndex = interim_MaxIndex;
        iNode = interim_iNode;
        indexlevel = level;

        pCount = interim_pCount;
        pNode = interim_pNode;
        chrono::high_resolution_clock::time_point end_ = chrono::high_resolution_clock::now();
        chrono::duration<double, std::nano> diff = end_ - start_;
        print_log(LV_INFO, "Expand NV-Tree cost %lf s.\n", diff.count() * 1e-9);
        print_log(LV_INFO, "End. MaxIndex = %d, pCount = %d level = %d.", MaxIndex, pCount, indexlevel);
    }

    void generateNextLeaf(LeafNode *leaf, uint64_t &sep)
    {
        // 1. tmp_leaf leaf，创建新的leaf 和 nextleaf。
        LeafNode *next = new (node_alloc->Allocate(sizeof(LeafNode))) LeafNode();
        void *mem = nullptr;
        posix_memalign(&mem, 64, sizeof(LeafNode));
        assert(mem != nullptr);
        LeafNode *tmp_leaf = new (mem) LeafNode();
        // LeafNode *tmp_leaf = new LeafNode();
        memcpy(tmp_leaf, leaf, sizeof(LeafNode));
        //
        int split = tmp_leaf->nElements / 2;

        leaf->nElements = split;
        next->nElements = tmp_leaf->nElements - split;

        for (int i = 0; i < next->nElements; i++)
        {
            next->elements[i] = tmp_leaf->elements[i + split];
        }
        next->next = leaf->next;
        leaf->next = next;
        sep = leaf->elements[split - 1].key;

        nvm_persist(next, sizeof(LeafNode));

        // uint64_t max_key = leaf->Get_MaxKey();
        // if(sep < max_key) {
        //    leaf->Print();
        //    assert(0); 
        // }

//        std::cout << "Entry " << leaf->nElements << " " << next->nElements << std::endl;
        // delete tmp_leaf;
        free(mem);
    }

    void splitLeafNode(LeafNode *leaf, PLeafNode *parent) {
        std::map<uint64_t, std::pair<void *, uint8_t>> maps;
        for (int i = leaf->nElements-1; i >= 0; i--)
        {
            // if (maps.find(leaf->elements[i].key) == maps.end())
            // {
            //     maps.insert(std::make_pair(leaf->elements[i].key,
            //                                std::make_pair(leaf->elements[i].value, leaf->elements[i].flag)));
            // }
            maps.insert(std::make_pair(leaf->elements[i].key,
                                           std::make_pair(leaf->elements[i].value, leaf->elements[i].flag)));
        }
        LeafNode *tmp = leaf;
        tmp->nElements = 0;
        for (auto it : maps)
        {
            // if(it.second.second != OpDelete) {  // 最后一次操作不是 Delete
                tmp->elements[tmp->nElements].key = it.first;
                tmp->elements[tmp->nElements].value = it.second.first;
                tmp->elements[tmp->nElements].flag = it.second.second;
                tmp->nElements++;
            // }
        }

        if (tmp->nElements > LeafMaxEntry / 2)
        {
            // split
            uint64_t sep;
            generateNextLeaf(tmp, sep);
            lCount ++;

            if(parent->insert(sep, leaf->next) == true) {
                rebuild();
            }
        }
        nvm_persist(tmp, sizeof(LeafNode));
    }

    bool modify(uint64_t key, void * value, uint8_t flag) {
        int id = 0;
        PLeafNode *parent = find_pnode(key, id);

        std::lock_guard<std::mutex> lk(parent->mut);

        int pos = parent->binary_search(key);
        LeafNode *leaf = parent->LNs[pos];

        int entry = leaf->nElements;

        bool exists = false;

        // if(parent->Get_MaxKey() < key) {
        //     print_log(LV_DEBUG, "Max key is %16llx, key is %16llx", parent->Get_MaxKey(), key);
        //     PrintIndex();
        //     Print();
        //     assert(0);
        // }
        if(flag != OpInsert) {
            for(int i = entry - 1; i >= 0; i--) {
                if(leaf->elements[i].key == key) {
                    if(leaf->elements[i].flag == OpInsert) {
                        exists = true;
                    }
                    break;
                }
            }
            if(!exists) {
                return false;
            }
        }

        leaf->elements[entry].key = key; 
        leaf->elements[entry].value = value; 
        leaf->elements[entry].flag = flag; 

        leaf->nElements ++;

        if(leaf->nElements == LeafMaxEntry) {
            splitLeafNode(leaf, parent);
        } else {
            nvm_persist(&leaf->elements[entry], sizeof(Element));
            nvm_persist(&leaf->nElements, sizeof(uint16_t));
        }
        assert(leaf->nElements < LeafMaxEntry);
        return true;
    }

    bool insert(uint64_t key, void * value) {
        return modify(key, value, OpInsert);
    }

    bool update(uint64_t key, void * value)
    {
        return modify(key, value, OpUpdate);
    }

    bool remove(uint64_t key)
    {
        return modify(key, nullptr, OpDelete);
    }

    void *get(uint64_t key) {
        int id = 0;
        PLeafNode *parent = find_pnode(key, id);

        std::lock_guard<std::mutex> lk(parent->mut);

        int pos = parent->binary_search(key);
        LeafNode *leaf = parent->LNs[pos];

        for(int i = leaf->nElements - 1; i >= 0; i --) {
            if(leaf->elements[i].key == key) {
                // if(key == 0xc20369a413e28fc1) {
                //     print_log(LV_DEBUG, "key %llx, value %p flag %d, opDelete is %d", leaf->elements[i].key, 
                //             leaf->elements[i].value, leaf->elements[i].flag, OpDelete);
                // }
                if(leaf->elements[i].flag == OpDelete) {
                    // print_log(LV_DEBUG, "Leaf is deleted");
                    return nullptr;
                } else {
                    return leaf->elements[i].value;
                }
            }
        }
        return nullptr;
    }

    void sort_leaf(LeafNode *leaf, uint8_t index[]) {

        for(uint8_t i = 0; i < leaf->nElements; i++) {
            index[i] = i;
        }
        for(uint8_t i = 0; i < leaf->nElements - 1; i++) {
            for(uint8_t j =0; j < leaf->nElements - 1 - i; j ++) {
                if(leaf->elements[index[j]].key > leaf->elements[index[j+1]].key) {
                    uint8_t tmp = index[j];
                    index[j] = index[j+1];
                    index[j+1] = tmp;
                }
            }
        }
    }

    void scan(uint64_t key1, uint64_t key2, std::vector<string> &values, int &size) {
        int find_size = 0;

        int id = 0;
        PLeafNode *parent = find_pnode(key1, id);

        int pos = parent->binary_search(key1);

        LeafNode *leaf = parent->LNs[pos];

        while(leaf != nullptr) {
            std::map<uint64_t, std::pair<void *, uint8_t>> maps;
            for (int i = leaf->nElements-1; i >= 0; i--)
            {
                // if (maps.find(p->elements[i].key) == maps.end())
                // {
                //     maps.insert(std::make_pair(p->elements[i].key,
                //                             std::make_pair(p->elements[i].value, p->elements[i].flag)));
                // }
                maps.insert(std::make_pair(leaf->elements[i].key,
                                            std::make_pair(leaf->elements[i].value, leaf->elements[i].flag)));
            }

            for (auto it : maps)
            {
                // print_log(LV_DEBUG, "Get range key is %16llx, value %p, flag %d.", 
                //                     it.first, it.second.first, it.second.second);
                if(it.first > key2) {
                    size = find_size;
                    return;
                } else {
                    if(it.second.second != OpDelete) {  // 最后一次操作不是Delete
                        if(it.first >= key1 && it.second.first != nullptr) {
                            values.push_back(string((char *)(it.second.first), NVM_ValueSize));
                            find_size ++;
                        }
                        if(find_size >= size) {
                            return ;
                        }
                    }
                }
            }
            leaf = leaf->next;
        }


        // while(id < pCount) {
        //     // std::lock_guard<std::mutex> lk(parent->mut);
        //     for(int i = 0; i < parent->n_keys; i++) {
        //         // LeafNode *leaf = parent->LNs[i];
        //         LeafNode tmp_leaf;
        //         LeafNode *leaf = &tmp_leaf;
        //         memcpy(leaf, parent->LNs[i], sizeof(LeafNode));

        //         std::map<uint64_t, std::pair<void *, uint8_t>> maps;
        //         for (int i = leaf->nElements-1; i >= 0; i--)
        //         {
        //             if (maps.find(leaf->elements[i].key) == maps.end())
        //             {
        //                 maps.insert(std::make_pair(leaf->elements[i].key,
        //                                         std::make_pair(leaf->elements[i].value, leaf->elements[i].flag)));
        //             }
        //         }

        //         for (auto it : maps)
        //         {
        //             // print_log(LV_DEBUG, "Get range key is %16llx, value %p, flag %d.", 
        //             //                     it.first, it.second.first, it.second.second);
        //             if(it.first > key2) {
        //                 size = find_size;
        //                 return;
        //             } else {
        //                 if(it.second.second != OpDelete) {  // 最后一次操作不是Delete
        //                     if(it.first >= key1 && it.second.first != nullptr) {
        //                         values.push_back(string((char *)(it.second.first), NVM_ValueSize));
        //                         find_size ++;
        //                     }
        //                     if(find_size >= size) {
        //                         return ;
        //                     }
        //                 }
        //             }
        //         }
        //     }
        //     id ++;
        //     parent ++;
        // }
        size = find_size;
    }

    void PrintIndex() {
        int level_start = 0;
        int level_count = 1;
        for(int i =0; i < indexlevel; i ++) {
            print_log(LV_INFO, "Index level %d print.", i);
            for(int j = 0; j < level_count; j ++) {
                iNode[level_start + j].Print();
            }
            level_start += level_count;
            level_count *= IndexWay;
        }
    }

    void Print() {
        for(int i = 0; i < pCount; i ++) {
            pNode[i].Print();
        }
    }

    void PrintInfo() {
        print_log(LV_INFO, "This is a NV-Tree.");
        print_log(LV_INFO, "Leaf node size is %d, leaf max entry is %d, ememt size is %d.", 
                sizeof(LeafNode), LeafMaxEntry, sizeof(Element));
        print_log(LV_INFO, "Parent node size is %d, parent max entry is %d.", sizeof(PLeafNode), NTMAX_WAY);
        print_log(LV_INFO, "Index node size is %d, index max entry is %d.", sizeof(IndexNode), IndexWay);
        uint64_t inode_space = MaxIndex * sizeof(IndexNode);
        uint64_t pnode_space = pCount * sizeof(PLeafNode);
        uint64_t leaf_space =  lCount * sizeof(LeafNode);
        uint64_t total_space =  inode_space + pnode_space + leaf_space;

        print_log(LV_INFO, "Index node storage used is %dG %dM %dK %dB", inode_space >> 30, inode_space >> 20 & (1024 - 1), 
                        inode_space >> 10 & (1024 - 1), inode_space & (1024 - 1));
        print_log(LV_INFO, "Parent node storage used is %dG %dM %dK %dB", pnode_space >> 30, pnode_space >> 20 & (1024 - 1), 
                        pnode_space >> 10 & (1024 - 1), pnode_space & (1024 - 1));
        print_log(LV_INFO, "Leaf node storage used is %dG %dM %dK %dB", leaf_space >> 30, leaf_space >> 20 & (1024 - 1), 
                        leaf_space >> 10 & (1024 - 1), leaf_space & (1024 - 1));
                        
        print_log(LV_INFO, "Total key storage used is %dG %dM %dK %dB", total_space >> 30, total_space >> 20 & (1024 - 1), 
                        total_space >> 10 & (1024 - 1), total_space & (1024 - 1));
    }

};