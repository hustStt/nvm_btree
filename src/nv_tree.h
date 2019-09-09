
#pragma once 

#include <climits>
#include <future>
#include <mutex>

#include "nvm_common.h"

const int NV_NodeSize = 256;
const int NTMAX_WAY = (NV_NodeSize - sizeof(void *) - 2) / (sizeof(uint64_t) + sizeof(void *));
const int IndexWay = (NV_NodeSize - 2) / sizeof(uint64_t);
const int LeafMaxEntry = (NV_NodeSize - sizeof(void *) - 2) / sizeof(Element);
const float Minimal_Fill_Factor = 0.5

enum OpFlag {
    OpInsert = 0;
    OpUpdate;
    OpDelete;
};

struct Element {
    uint8_t flag;
    uint64_t key;
    void *value;
    Element(uint8_t flag, const char * &key){
        SetFlag(flag);
        SetKey(key);
    }
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

class PLeafNode;
class IndexNode;

class LeafNode {
public:
    int16_t nElements;
    Element [LeafMaxEntry];
    LeafNode* next;
public:
    LeafNode() {
        nElements = 0;
        next = nullptr;
    }

    ~LeafNode() {

    }
    int16_t GetnElements(){
        return nElements;
    }

    void SetnElements(int16_t nEle){
        nElements = nEle;
    }

    Element GetElement(int off){
        return elements[off];
    }

    void SetElement(int off, Element e){
        elements[off] = e;
    }

    bool IsFull(){
        return nElements == LeafMaxEntry;
    }

    char *find_max_key() {
        char *max_key = nullptr;
        for(int j = 0; j < nElements; j ++) {
            if(max_key == nullptr) {
                max_key = elements[j].key;
            }
            else if(KeyCompare(max_key, elements[j].key) < 0) {
                max_key = elements[j].key;
            }
        }
        return max_key;
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
            if (keys[mid] >= key)
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

    bool IsFull(){
        return n_keys == NTMAX_WAY;
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

    void SetKey_nodrain(int off, uint64_t key) {
        m_key[off] =  key;
    }

    LeafNode * GetLN(int off){
        return LNs[off];
    }

    void SetLN(int off, LeafNode* ln){
        LNs[off] = ln;
    }

    int16_t Getn(){
        return n_keys;
    }

    bool insert(uint64_t key, LeafNode *child) {
        int d = binary_search(key);
        if (m_key[d] == key)
        {
            assert(d >= children_number - 1);
        }
        for (int i = children_number - 1; i > d; i--)
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
        return false;
    }

    void Setn(int16_t n){
        n_keys = n;
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
            if (keys[mid] >= key)
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

    void SetKey_nodrain(int off, uint64_t key) {
        m_key[off] = key;
    }

    uint64_t Get_MaxKey() {
        if(n_keys == 0) {
            return (uint64_t) -1;
        }
        return m_key[n_keys -1];
    } 

    int16_t Getn(){
        return n_keys;
    }

    void Setn(int16_t n){
        n_keys = n;
    }
} __attribute__((aligned(64)));

class NVTree {
    IndexNode *iNode;
    PLeafNode *pNode;
    int MaxIndex;
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

    PLeafNode *find_pnode(uint64_t key) {
        int pos = 0;
        while(id < MaxIndex) {
            pos = iNode[id].binary_search(key);
            id = id * IndexWay + 1 + pos;
        }
        id -= MaxIndex; 
        return (pNode + id);

    }

    void find_leaf(const char * &key, int &id, int &pos, PLeafNode *&parent, LeafNode *&leaf) {
        parent = find_pnode(key);

        pos = parent->binary_search(key);
        leaf = parent->LNs[pos];
    }

    void rebuild() {
        int level = 0;
        int tmp_count = lCount;
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
            }
        }
        assert(interim_pCount != lCount);
        int interim_MaxIndex = index_start[j] + index_count[j];
        IndexNode *interim_iNode = (PLeafNode *)IndexNode->Allocate(sizeof(PLeafNode) * interim_MaxIndex);

        {
            int i = level - 1 
            for(int j = 0; j < index_count[i]; j ++) {
                int id = index_start[i] + j;
                IndexNode *tmp_iNode =  interim_iNode + index_start[i] + j;
                tmp_iNode->n_keys = 0;
                for(int k = 0; k < IndexWay; k++) {
                    int pindex = id * IndexWay + k + 1;
                    if(pindex >= interim_pCount) {
                        pindex = interim_pCount -1;
                    }
                    PLeafNode *p = interim_pNodeid * IndexWay + pindex;
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

        pCount = interim_pCount;
        pNode = interim_pNode;
    }

    void generateNextLeaf(LeafNode *leaf, uint64_t &sep)
    {
        // 1. log leaf，创建新的leaf 和 nextleaf。
        LeafNode *next = new (alloc_leaf()) LeafNode;
        LeafNode *log = new LeafNode();
        memcpy(log, leaf, sizeof(LeafNode));
        //
        int split = log->entry / 2;

        leaf->persist_entry = leaf->entry = split;
        next->persist_entry = next->entry = log->entry - split;

        for (int i = 0; i < next->entry; i++)
        {
            next->elements[i] = log->elements[i + split];
        }
        next->next = leaf->next;
        leaf->next = next;
        sep = leaf->elements[split - 1].key;

//        std::cout << "Entry " << leaf->entry << " " << next->entry << std::endl;
        delete log;
    }

    void splitLeafNode(LeafNode *leaf, PLeafNode *parent) {
        std::map<uint64_t, std::pair<void *, uint8_t>> maps;
        for (int i = leaf->nElements-1; i >= 0; i--)
        {
            if (maps.find(leaf->elements[i].key) == maps.end())
            {
                maps.insert(std::make_pair(leaf->elements[i].key,
                                           std::make_pair(leaf->elements[i].value, leaf->elements[i].flag)));
            }
        }
        LeafNode *tmp = leaf;
        tmp->nElements = 0;
        for (auto it : maps)
        {
            tmp->elements[tmp->nElements].key = it.first;
            tmp->elements[tmp->nElements].value = it.second.first;
            tmp->elements[tmp->nElements].flag = it.second.second;
            tmp->nElements++;
        }

        if (maps.size() > LeafMaxEntry / 2)
        {
            // split
            uint64_t sep;
            generateNextLeaf(tmp, sep);
            lCount ++;

            if(parent->insert(sep, leaf->next) == true) {
                rebuild();
            }
        }
    }

    bool modify(uint64_t key, void * value, uint8_t flag) {
        PLeafNode *parent = find_pnode(key);

        int pos = parent->binary_search(key);
        LeafNode *leaf = parent->LNs[pos];

        int entry = leaf->n_keys;

        bool exists = false;

        for(int i = entry - 1; i >= 0; i--) {
            if(leaf->elements[i].key == key) {
                if(leaf->elements[i].flag == OpInsert) {
                    exists = true;
                }
                break;
            }
        }
        if(flag == OpInsert) {
            if(exists) {
                return false;
            }
        } else if(!exists) {
            return false;
        }

        leaf->elements[entry].key = key; 
        leaf->elements[entry].value = value; 
        leaf->elements[entry].flag = flag; 

        leaf->n_keys ++;

        if(leaf->n_keys == LeafMaxEntry) {
            splitLeafNode(leaf, parent);
        }
        assert(leaf->n_keys < LeafMaxEntry);
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
        return modify(key, value, OpDelete);
    }

    void Print() {

    }

    void PrintInfo() {
        print_log(LV_INFO, "This is a NV-Tree.");
        print_log(LV_INFO, "Leaf node size is %d, leaf max entry is %d.", sizeof(LeafNode), LeafMaxEntry);
        print_log(LV_INFO, "Parent node size is %d, parent max entry is %d.", sizeof(PLeafNode), NTMAX_WAY);
        print_log(LV_INFO, "Index node size is %d, index max entry is %d.", sizeof(IndexNode), IndexWay);
    }

};