
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
const int NTMAX_WAY = (NV_NodeSize - sizeof(void *) - 2) / (sizeof(uint64_t) + sizeof(void *));
const int IndexWay = (NV_NodeSize - 2) / sizeof(uint64_t);
const int LeafMaxEntry = (NV_NodeSize - sizeof(void *) - 2) / sizeof(Element);

class PLeafNode;
class IndexNode;
class NVTree;

class LeafNode {
    int16_t nElements;
    Element elements[LeafMaxEntry];
    LeafNode* next;
public:
    friend class NVTree;
    LeafNode() {
        nElements = 0;
        next = nullptr;
    }

    ~LeafNode() {

    }
};


class PLeafNode {
    int16_t n_keys;
    uint64_t m_key[NTMAX_WAY];
    LeafNode *LNs[NTMAX_WAY + 1];

public:
    friend class NVTree;
    PLeafNode() {
        n_keys = 0;
    }

    ~PLeafNode() {

    }
    
    
};


class IndexNode {
    int16_t n_keys;
    uint64_t m_key[IndexWay];

public:
    friend class NVTree;

    IndexNode() {
        n_keys = 0;
    }

    ~IndexNode() {

    }

};

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

    
    bool modify(uint64_t key, void *value, uint8_t flag) {
        return true;
    }

    bool insert(uint64_t key, void *value) {
        return modify(key, value, OpInsert);
    }

    bool update(uint64_t key, void *value)
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