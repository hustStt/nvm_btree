/*
   Copyright (c) 2018, UNIST. All rights reserved. The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST.

   Please use at your own risk.
*/
#pragma once 

#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#define PAGESIZE (512)

#define CACHE_LINE_SIZE 64

#define IS_FORWARD(c) (c % 2 == 0)

class nvmpage;
class subtree;
class btree;

POBJ_LAYOUT_BEGIN(btree);
//POBJ_LAYOUT_ROOT(btree, subtree);
POBJ_LAYOUT_TOID(btree, nvmpage);
POBJ_LAYOUT_TOID(btree, subtree);
POBJ_LAYOUT_END(btree);

using entry_key_t = int64_t;

using namespace std;

class nvmheader {
private:
  TOID(nvmpage) sibling_ptr; // 16 bytes
  nvmpage *leftmost_ptr;     // 8 bytes
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes

  friend class nvmpage;
  friend class subtree;

public:
  void constructor() {
    leftmost_ptr = NULL;
    TOID_ASSIGN(sibling_ptr, pmemobj_oid(this));
    sibling_ptr.oid.off = 0;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }
};

class nvmentry {
private:
  entry_key_t key; // 8 bytes
  char *ptr;       // 8 bytes

public:
  void constructor() {
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class nvmpage;
  friend class btree;
  friend class subtree;
};

const int nvm_cardinality = (PAGESIZE - sizeof(nvmheader)) / sizeof(nvmentry);
const int nvm_count_in_line = CACHE_LINE_SIZE / sizeof(nvmentry);

class nvmpage {
private:
  nvmheader hdr;                 // nvmheader in persistent memory, 32 bytes
  nvmentry records[nvm_cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class btree;
  friend class subtree;

  void constructor(uint32_t level);
  void constructor(PMEMobjpool *pop, nvmpage *left, entry_key_t key, nvmpage *right,
                  uint32_t level);
  int count();

  bool remove_key(PMEMobjpool *pop, entry_key_t key);
  bool remove(btree *bt, entry_key_t key, bool only_rebalance,
            bool with_lock, subtree* sub_root);
  void insert_key(PMEMobjpool *pop, entry_key_t key, char *ptr,
                        int *num_entries, bool flush,
                        bool update_last_index);
  nvmpage *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
            subtree *sub_root, nvmpage *invalid_sibling);

  void linear_search_range(entry_key_t min, entry_key_t max,
                          unsigned long *buf);
  char *linear_search(entry_key_t key);

  void print();
  void printAll();
};

class subtree {
  private:
    bpnode* dram_ptr;
    nvmpage* nvm_ptr; // off 
    subtree* sibling_ptr; // off
    uint64_t heat;
    PMEMobjpool *pop;
    NVMAllocator* log_alloc;
    bool flag;
    // true:dram   false:nvm
  public:
    void constructor(PMEMobjpool *pop, bpnode* dram_ptr, subtree* next = nullptr, uint64_t heat = 0, bool flag = true) {
      this->flag = flag;
      this->dram_ptr = dram_ptr;
      this->nvm_ptr = nullptr;
      this->heat = heat;
      this->pop = pop;
      this->sibling_ptr = next;

      pmemobj_persist(pop, this, sizeof(subtree));
    }

    void constructor(PMEMobjpool *pop, nvmpage* nvm_ptr, subtree* next = nullptr, uint64_t heat = 0, bool flag = false) {
      this->flag = flag;
      this->dram_ptr = nullptr;
      this->nvm_ptr = nvm_ptr;
      this->heat = heat;
      this->pop = pop;
      this->sibling_ptr = next;

      pmemobj_persist(pop, this, sizeof(subtree));
    }

    void subtree_insert(btree* root, entry_key_t key, char* right);
    void subtree_delete(btree* root, entry_key_t);
    char *subtree_search(entry_key_t);
    void subtree_search_range(entry_key_t, entry_key_t, unsigned long *); 
    //void subtree_search_range(entry_key_t, entry_key_t, std::vector<std::string> &values, int &size); 
    void subtree_search_range(entry_key_t, entry_key_t, void **values, int &size); 

    void btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level, btree* bt);
    void btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
        bool *is_leftmost_node, bpnode **left_sibling, btree* bt);

    // nvm --> dram
    char* DFS(nvmpage* root);
    void nvm_to_dram();

    // dram --> nvm
    char* DFS(char* root);
    void dram_to_nvm();

    // sync dram --> nvm
    void sync_subtree();

    nvmpage *to_nvmpage(nvmpage *off) {
      return (nvmpage *)((uint64_t)off + (uint64_t)pop);
    }

    nvmpage *to_nvmpage(char *off) {
      return (nvmpage *)((uint64_t)off + (uint64_t)pop);
    }

    nvmpage *get_nvmroot_ptr() {
      return to_nvmpage(nvm_ptr);
    }

    friend class bpnode;
};

static subtree* newSubtreeRoot(PMEMobjpool *pop, bpnode *subtree_root, subtree * next = nullptr) {
    TOID(subtree) node = TOID_NULL(subtree);
    POBJ_NEW(pop, &node, subtree, NULL, NULL);
    D_RW(node)->constructor(pop, subtree_root, next);
    return D_RW(node);
}

static subtree* newSubtreeRoot(PMEMobjpool *pop, nvmpage *subtree_root, subtree * next = nullptr) {
    TOID(subtree) node = TOID_NULL(subtree);
    POBJ_NEW(pop, &node, subtree, NULL, NULL);
    D_RW(node)->constructor(pop, subtree_root, next);
    return D_RW(node);
}
