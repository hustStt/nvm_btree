
#pragma once

/*
* nvm_btree.h
*/

#include <array>
#include <thread>
#include <mutex>
#include <string>
#include <assert.h>

#include "nvm_common.h"
#include "nvm_allocator.h"

#include "single_btree.h"

#include "single_pmdk.h"
using namespace std;

#define NODEPATH   "/mnt/pmem1/persistent"
#define LOGPATH "/mnt/pmem1/log_persistent"

const uint64_t NVM_NODE_SIZE = 1 * (1ULL << 30);
const uint64_t NVM_LOG_SIZE = 30 * (1ULL << 30);

class NVMBtree{
public:
    NVMBtree(string &path);
    ~NVMBtree();

    void Insert(const unsigned long key, const string &value);

    void Delete(const unsigned long key);

    const std::string Get(const unsigned long key);

    void GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size);

    void Insert(const unsigned long key, char *pvalue);
    int Get(const unsigned long key, char *&pvalue);

    void GetRange(unsigned long key1, unsigned long key2, void ** pvalues, int &size);

    void FunctionTest(int ops);
    void motivationtest();

    void Print();
    void PrintInfo();
/*
    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    void PrintStorage() {
        node_alloc->PrintStorage();
        value_alloc->PrintStorage();
    }
*/
    void test() {
        mybt->closeChange();
    }

    void printfLeaf() {
        bt->scan_all_leaf();
    }

    void recovery() {
        mybt->Recover(mybt->getPoolPtr());
    }

private:
    btree *bt;
    MyBtree *mybt;
};