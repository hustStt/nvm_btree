#pragma once

/*
* nvm_nvtree.h
*/

#include <array>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include <assert.h>

#include "nvm_common.h"
#include "nvm_allocator.h"
#include "nv_tree.h"

class NVMNvtree {
    public:
    NVMNvtree() {
        nvtree = new NVTree();
        if(nvtree == nullptr) {
            print_log(LV_ERR, "Initial faild.");
        }
    }

    ~NVMNvtree() {
        delete nvtree;
    }

    void Insert(const unsigned long key, const string &value) {
        if(nvtree) {
            char *pvalue = value_alloc->Allocate(value.size());
            nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
            nvtree->insert(key, pvalue);
        }
    }

    void Delete(const unsigned long key) {
        if(nvtree) {
            nvtree->remove(key);
        }
    }

    void Update(const unsigned long key, const string &value) {
        if(nvtree) {
            char *pvalue = value_alloc->Allocate(value.size());
            nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
            nvtree->update(key, pvalue);
        }
    }

    const std::string Get(const unsigned long key) {
        if(nvtree) {
            char *pvalue = (char *)nvtree->get(key);
            // print_log(LV_DEBUG, "Get value pointer is %p", pvalue);
            if(pvalue == nullptr) {
                return ""; 
            }
            return string(pvalue, NVM_ValueSize);
        }
        return "";
    }

    void GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
        if(nvtree) {
            nvtree->scan(key1, key2, values, size);
        }
    }

    void Print() {
        if(nvtree) {
            nvtree->Print();
        }
    }

    void PrintInfo() {
        if(nvtree) {
            nvtree->PrintInfo();
            show_persist_data();
        }
    }

    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    void PrintStorage() {
        node_alloc->PrintStorage();
        value_alloc->PrintStorage();
    }

private:
    NVTree *nvtree;
};
