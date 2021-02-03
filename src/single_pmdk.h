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
#include <queue>

#include "nvm_common.h"

#define PAGESIZE (512)

#define CACHE_LINE_SIZE 64

#define IS_FORWARD(c) (c % 2 == 0)

#define IS_VALID_PTR(p) (((uint64_t)p & 0x700000000000) == 0x700000000000) 

static inline int file_exists(char const *file) { return access(file, F_OK); }

class nvmpage;
class subtree;
class btree;
class bpnode;
class MyBtree;

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_ROOT(btree, MyBtree);
POBJ_LAYOUT_TOID(btree, nvmpage);
POBJ_LAYOUT_TOID(btree, subtree);
POBJ_LAYOUT_END(btree);

using entry_key_t = int64_t;

using namespace std;

class nvmheader {
private:
  nvmpage *leftmost_ptr;     // 8 bytes
  nvmpage *sibling_ptr; // 8 bytes
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes
  char * none;          //8 bytes

  friend class nvmpage;
  friend class bpnode;
  friend class subtree;
  friend class btree;

public:
  void constructor() {
    leftmost_ptr = NULL;
    //TOID_ASSIGN(sibling_ptr, pmemobj_oid(this));
    //sibling_ptr.oid.off = 0;
    sibling_ptr = nullptr;
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
  friend class bpnode;
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
  friend class bpnode;

  void constructor(uint32_t level = 0) {
    hdr.constructor();
    for (int i = 0; i < nvm_cardinality; i++) {
      records[i].key = LONG_MAX;
      records[i].ptr = NULL;
    }

    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  void constructor(PMEMobjpool *pop, nvmpage *left, entry_key_t key, nvmpage *right,
                    uint32_t level = 0) {
    hdr.constructor();
    for (int i = 0; i < nvm_cardinality; i++) {
      records[i].key = LONG_MAX;
      records[i].ptr = NULL;
    }

    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;

    pmemobj_persist(pop, this, sizeof(nvmpage));
  }

  inline int count() {
    uint8_t previous_switch_counter;
    int count = 0;

    do {
      previous_switch_counter = hdr.switch_counter;
      count = hdr.last_index + 1;

      while (count >= 0 && records[count].ptr != NULL) {
        if (IS_FORWARD(previous_switch_counter))
          ++count;
        else
          --count;
      }

      if (count < 0) {
        count = 0;
        while (records[count].ptr != NULL) {
          ++count;
        }
      }

    } while (previous_switch_counter != hdr.switch_counter);

    return count;
  }

  inline bool remove_key(PMEMobjpool *pop, entry_key_t key) {
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i) {
      if (!shift && records[i].key == key) {
        records[i].ptr =
            (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift) {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;

        // flush
        uint64_t records_ptr = (uint64_t)(&records[i]);
        int remainder = records_ptr % CACHE_LINE_SIZE;
        bool do_flush =
            (remainder == 0) ||
            ((((int)(remainder + sizeof(nvmentry)) / CACHE_LINE_SIZE) == 1) &&
              ((remainder + sizeof(nvmentry)) % CACHE_LINE_SIZE) != 0);
        if (do_flush) {
          pmemobj_persist(pop, (void *)records_ptr, CACHE_LINE_SIZE);
        }
      }
    }

    if (shift) {
      --hdr.last_index;
    }
    return shift;
  }


  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false,
            bool with_lock = true, subtree* sub_root = NULL);
  bool merge(btree *bt, bpnode *left_sibling, entry_key_t deleted_key_from_parent, subtree* sub_root, subtree* left_subtree_sibling);

  nvmpage *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
            subtree *sub_root = NULL, nvmpage *invalid_sibling = NULL);

  void insert_key(PMEMobjpool *pop, entry_key_t key, char *ptr,
                      int *num_entries, bool flush = true,
                      bool update_last_index = true);


  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size, uint64_t base = 0);

  void linear_search_range(entry_key_t min, entry_key_t max,
                            unsigned long *buf, uint64_t base = 0) {
    int i, off = 0;
    uint8_t previous_switch_counter;
    nvmpage *current = this;

    while (current) {
      int old_off = off;
      do {
        previous_switch_counter = current->hdr.switch_counter;
        off = old_off;

        entry_key_t tmp_key;
        char *tmp_ptr;

        if (IS_FORWARD(previous_switch_counter)) {
          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else {
              return;
            }
          }

          for (i = 1; current->records[i].ptr != NULL; ++i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else {
                return;
              }
            }
          }
        } else {
          for (i = current->count() - 1; i > 0; --i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else {
                return;
              }
            }
          }

          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else {
              return;
            }
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      // todo
      if (IS_VALID_PTR(current->hdr.sibling_ptr) || base == 0) {
        current = current->hdr.sibling_ptr;
      } else {
        current = (nvmpage *)((uint64_t)current->hdr.sibling_ptr + base);
      }
    }
  }

  char *linear_search(entry_key_t key) {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter)) {
          if ((k = records[0].key) == key) {
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          for (i = count() - 1; i > 0; --i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            if ((k = records[0].key) == key) {
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        return ret;
      }
      /*
      if ((t = (char *)hdr.sibling_ptr.oid.off) &&
          key >= D_RW(hdr.sibling_ptr)->records[0].key) {
        return t;
      }
      */
      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; i <= hdr.last_index && records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                //printf("i: %d last_index: %d key: %ld  ret: %x\n", i , hdr.last_index, key, ret);
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            //printf("i: %d last_index: %d key: %ld  ret: %x\n", i , hdr.last_index, key, ret);
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);
      /*
      if ((t = (char *)hdr.sibling_ptr.oid.off) != NULL) {
        if (key >= D_RW(hdr.sibling_ptr)->records[0].key)
          return t;
      }*/

      if (ret) {
        //printf("key: %ld  ret: %x", key, ret);
        return ret;
      } else
        return (char *)hdr.leftmost_ptr;
    }

    return NULL;
  }

  // print a node
  void print() {
    if (hdr.leftmost_ptr == NULL)
      printf("[%d] leaf %x \n", this->hdr.level, (uint64_t)pmemobj_oid(this).off);
    else
      printf("[%d] internal %x \n", this->hdr.level, pmemobj_oid(this).off);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if (IS_FORWARD(hdr.switch_counter))
      printf("->\n");
    else
      printf("<-\n");

    if (hdr.leftmost_ptr != NULL)
      printf("%x ", hdr.leftmost_ptr);

    for (int i = 0; records[i].ptr != NULL; ++i)
      printf("%ld,%x ", records[i].key, records[i].ptr);

    printf("%x ", (uint64_t)hdr.sibling_ptr);

    printf("\n");
  }

  void printAll() {
    TOID(nvmpage) p = TOID_NULL(nvmpage);
    TOID_ASSIGN(p, pmemobj_oid(this));

    if (hdr.leftmost_ptr == NULL) {
      printf("printing leaf node: ");
      print();
    } else {
      printf("printing internal node: ");
      print();
      p.oid.off = (uint64_t)hdr.leftmost_ptr;
      D_RW(p)->printAll();
      for (int i = 0; records[i].ptr != NULL; ++i) {
        p.oid.off = (uint64_t)records[i].ptr;
        D_RW(p)->printAll();
      }
    }
  }
};

class RebalanceTask {
  public:
    subtree * left;
    subtree * right;
    bpnode * cur_d; 
    nvmpage * cur_n;
    entry_key_t deleted_key_from_parent;

    RebalanceTask(subtree * left, subtree * right, bpnode * cur_d, nvmpage * cur_n, entry_key_t deleted_key_from_parent) {
      this->left = left;
      this->right = right;
      this->cur_d = cur_d; 
      this->cur_n = cur_n;
      this->deleted_key_from_parent = deleted_key_from_parent;
    }
};

class MyBtree{
  private:
    PMEMobjpool *pop;
    subtree * head;  // off
    int time_;       // sec
    int subtree_num; // dram subtree num
    btree * bt; 
    bool switch_;
    static MyBtree * mybt;
  public:
    static MyBtree *getInitial(string persistent_path = "") {
      if (mybt == nullptr) {
        TOID(MyBtree) nvmbt = TOID_NULL(MyBtree);
        PMEMobjpool *pop;
        if (file_exists(persistent_path.c_str()) != 0) {
            pop = pmemobj_create(persistent_path.c_str(), "btree", 30000000000,
                                0666); // make 30GB memory pool
            nvmbt = POBJ_ROOT(pop, MyBtree);
            D_RW(nvmbt)->constructor(pop);
        } else {
            pop = pmemobj_open(persistent_path.c_str(), "btree");
            nvmbt = POBJ_ROOT(pop, MyBtree);
            D_RW(nvmbt)->Recover(pop);
        }
        mybt = D_RW(nvmbt);
      }
      return mybt;
    }

    btree * getBt() {
      return bt;
    }

    void setHead(subtree * head) {
      this->head = head;
      pmemobj_persist(pop, &(this->head), sizeof(subtree *));
    }

    inline subtree *to_nvmptr(subtree *off) {
      if (off == nullptr) return nullptr;
      return (subtree *)((uint64_t)off + (uint64_t)pop);
    }

    void constructor(PMEMobjpool * pool);
    void Recover(PMEMobjpool *);
    void Redistribute();
    void later();
    void exitBtree();
    void closeChange() {
      switch_ = false;
    }
};

class subtree {
  private:
    bpnode* dram_ptr;
    nvmpage* nvm_ptr; // off 
    subtree* sibling_ptr; // off
    uint64_t heat;
    PMEMobjpool *pop;
    LogAllocator* log_alloc;
    RebalanceTask *rt;
    bool flag;
    bool change;
    bool lock;
    // true:dram   false:nvm
  public:
    void constructor(PMEMobjpool *pop, bpnode* dram_ptr, subtree* next = nullptr, uint64_t heat = 0, bool flag = true) {
      this->flag = flag;
      this->dram_ptr = dram_ptr;
      this->nvm_ptr = nullptr;
      this->heat = heat;
      this->pop = pop;
      this->sibling_ptr = next;
      this->log_alloc = getNewLogAllocator();
      this->rt = nullptr;
      this->change = flag;
      this->lock = false;

      pmemobj_persist(pop, this, sizeof(subtree));
    }

    void constructor(PMEMobjpool *pop, nvmpage* nvm_ptr, subtree* next = nullptr, uint64_t heat = 0, bool flag = false) {
      this->flag = flag;
      this->dram_ptr = nullptr;
      this->nvm_ptr = nvm_ptr;
      this->heat = heat;
      this->pop = pop;
      this->sibling_ptr = next;
      this->log_alloc = nullptr;
      this->rt = nullptr;
      this->change = flag;
      this->lock = false;

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
    void btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
        bool *is_leftmost_node, nvmpage **left_sibling, btree* bt);

    // nvm --> dram
    char* DFS(nvmpage* root, bpnode **pre);
    void nvm_to_dram(bpnode **pre);

    // dram --> nvm
    char* DFS(char* root, nvmpage **pre, bool ifdel = true);
    void dram_to_nvm(nvmpage **pre);

    // sync dram --> nvm
    void sync_subtree(nvmpage **pre);

    inline nvmpage *to_nvmpage(nvmpage *off) {
      if (off == nullptr)  return nullptr;
      return (nvmpage *)((uint64_t)off + (uint64_t)pop);
    }

    inline nvmpage *to_nvmpage(char *off) {
      if (off == nullptr)  return nullptr;
      return (nvmpage *)((uint64_t)off + (uint64_t)pop);
    }

    inline nvmpage *get_nvmroot_ptr() {
      return to_nvmpage(nvm_ptr);
    }

    uint64_t getHeat() {
      return heat;
    }

    void setHeat(uint64_t heat) {
      this->heat = heat;
      //persist
    }

    void increaseHeat() {
      ++heat;
      //persist
    }

    subtree * getSiblingPtr() {
      return sibling_ptr;
    }

    bool isNVMBtree() {
      return !flag;
    }

    bool rebalance(btree * bt);

    bool needRebalance() {
      if (rt != nullptr) {
        return true;
      }
      return false;
    }

    void deleteRt() {
      delete rt;
      rt = nullptr;
    }

    bool getState() {
      return change;
    }

    void flushState() {
      change = false;
    }

    void PrintInfo() {
      printf("subtree: %p is dram: %d  heat: %lu\n", this, flag, heat);
      if(log_alloc) { 
        log_alloc->PrintStorage();
      }
    }

    bpnode *getLastDDataNode();
    nvmpage *getLastNDataNode();
    bpnode *getDramDataNode(char *ptr);
    nvmpage *getNvmDataNode(char *ptr);

    friend class bpnode;
    friend class nvmpage;
    friend class MyBtree;
    friend class btree;
};

static subtree* newSubtreeRoot(PMEMobjpool *pop, bpnode *subtree_root, subtree * pre = nullptr) {
    TOID(subtree) node = TOID_NULL(subtree);
    POBJ_NEW(pop, &node, subtree, NULL, NULL);
    if (pre) {
      // 分裂生成
      D_RW(node)->constructor(pop, subtree_root, pre->getSiblingPtr(), pre->getHeat() / 2);
    } else {
      // 新生成
      D_RW(node)->constructor(pop, subtree_root);
    }
    return D_RW(node);
    // subtree *node = new subtree;
    // node->constructor(pop, subtree_root, next);
    // return node;
}

static subtree* newSubtreeRoot(PMEMobjpool *pop, nvmpage *subtree_root, subtree * pre = nullptr) {
    TOID(subtree) node = TOID_NULL(subtree);
    POBJ_NEW(pop, &node, subtree, NULL, NULL);
    if (pre) {
      // 分裂生成
      D_RW(node)->constructor(pop, subtree_root, pre->getSiblingPtr(), pre->getHeat() / 2);
    } else {
      // 新生成
      D_RW(node)->constructor(pop, subtree_root);
    }
    return D_RW(node);
    // subtree *node = new subtree;
    // node->constructor(pop, subtree_root, next);
    // return node;
}

struct cmp {
    bool operator()(subtree* a, subtree* b) {
        return a->getHeat() > b->getHeat();
    }
};