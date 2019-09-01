
#pragma once

/*
* nvm_btree.h
*/

#include <array>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <assert.h>

#include "nvm_common.h"
#include "nvm_allocator.h"
#include "skiplist.h"

using namespace std;

class NVMSkipList{
public:
    NVMSkipList() {
        slist = new  SkipList();   
    }

    ~NVMSkipList() {
        if(slist) {
            delete slist;
        }
    }

    void Insert(const unsigned long key, const string &value) {
        if(slist) {
            char *pvalue = value_alloc->Allocate(value.size());
            pmem_memcpy_persist(pvalue, value.c_str(), value.size());
            char key_[NVM_KeySize + sizeof(uint64_t)];
            fillchar8wirhint64(key_, key);
            uint64_t vpoint = (uint64_t)pvalue;
            memcpy(key_ + NVM_KeySize, &vpoint, sizeof(uint64_t));
            std::lock_guard<std::shared_timed_mutex> WriteLock(share_mut);
            slist->Insert(string(key_, NVM_KeyBuf));
        }
    }

    void Delete(const unsigned long key) {
        if(slist) {
            char key_[NVM_KeySize + sizeof(uint64_t)];
            fillchar8wirhint64(key_, key);
            std::lock_guard<std::shared_timed_mutex> WriteLock(share_mut);
            slist->Delete(string(key_, NVM_KeyBuf));
        }
    }

    const std::string Get(const unsigned long key) {
        if(slist) {
            char key_[NVM_KeySize + sizeof(uint64_t)];
            fillchar8wirhint64(key_, key);
            std::shared_lock<std::shared_timed_mutex> ReadLock(share_mut);
            return slist->Get(string(key_, NVM_KeyBuf));
        }
    }

    void GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
        if(slist) {
            char key1_[NVM_KeySize + sizeof(uint64_t)];
            char key2_[NVM_KeySize + sizeof(uint64_t)];
            fillchar8wirhint64(key1_, key1);
            fillchar8wirhint64(key2_, key2);
            std::shared_lock<std::shared_timed_mutex> ReadLock(share_mut);
            return slist->GetRange(string(key1_, NVM_KeyBuf), string(key2_, NVM_KeyBuf), values, size);
        }
    }

    void FunctionTest(int ops) {

    }
    void motivationtest() {

    }

    void Print() {

    }

    void PrintInfo() {

    }

    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    void PrintStorage() {
        node_alloc->PrintStorage();
        value_alloc->PrintStorage();
    }

private:
    SkipList *slist;
    std::shared_timed_mutex share_mut;
};
