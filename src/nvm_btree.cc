#include "nvm_btree.h"

NVMAllocator *node_alloc = nullptr;
NVMAllocator *value_alloc = nullptr;

int AllocatorInit(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize) {
    node_alloc = new NVMAllocator(path, keysize);
    if(node_alloc == nullptr) {
        return -1;
    }
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete node_alloc;
        return -1;
    }
    return 0;
}

void AllocatorExit() {
    if(node_alloc) {
        delete node_alloc;
    } 

    if(value_alloc) {
        delete value_alloc;
    }

}

NVMBtree::NVMBtree() {
    bt = new btree();
    if(!bt) {
        assert(0);
    }
    bpnode *root = NewBpnode();
    btree tmpbtree = btree(root);
}

NVMBtree::~NVMBtree() {
    if(bt) {
        delete bt;
    }
}
    
void NVMBtree::Insert(const unsigned long key, const string &value) {
    if(bt) {
        char *pvalue = value_alloc->Allocate(value.size());
        pmem_memcpy_persist(pvalue, value.c_str(), value.size());
        bt->btree_insert(key, pvalue);
    }
}

void NVMBtree::Delete(const unsigned long  key) {
    if(bt) {
        bt->btree_delete(key);
    }
}

const string NVMBtree::Get(const unsigned long key) {
    char *pvalue = NULL;
    if(bt) {
        pvalue = bt->btree_search(key);
    }
    if(pvalue) {
        // print_log(LV_DEBUG, "Get pvalue is %p.", pvalue);
        return string(pvalue, NVM_ValueSize);
    }
    return "";
}

void NVMBtree::GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
    if(bt) {
        bt->btree_search_range(key1, key2, values, size);
    }
}

void NVMBtree::Print() {
    if(bt) {
        bt->printAll();
    }
}

void NVMBtree::PrintInfo() {
    if(bt) {
        bt->PrintInfo();
    }
}

void NVMBtree::FunctionTest(int ops) {

}

void NVMBtree::motivationtest() {
    
}