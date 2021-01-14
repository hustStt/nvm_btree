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

#include "nvm_common.h"

#define PAGESIZE (512)

#define CACHE_LINE_SIZE 64

#define IS_FORWARD(c) (c % 2 == 0)

class nvmpage;
class subtree;
class btree;
class bpnode;

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
  nvmpage *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
            subtree *sub_root = NULL, nvmpage *invalid_sibling = NULL);


  inline void insert_key(PMEMobjpool *pop, entry_key_t key, char *ptr,
                        int *num_entries, bool flush = true,
                        bool update_last_index = true) {
    // update switch_counter
    if (!IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0) { // this nvmpage is empty
      nvmentry *new_entry = (nvmentry *)&records[0];
      nvmentry *array_end = (nvmentry *)&records[1];
      new_entry->key = (entry_key_t)key;
      new_entry->ptr = (char *)ptr;

      array_end->ptr = (char *)NULL;

      if (flush) {
        pmemobj_persist(pop, this, CACHE_LINE_SIZE);
      }
    } else {
      int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;

      if (flush) {
        if ((uint64_t) & (records[*num_entries + 1]) % CACHE_LINE_SIZE == 0)
          pmemobj_persist(pop, &records[*num_entries + 1].ptr, sizeof(char *));
      }

      // FAST
      for (i = *num_entries - 1; i >= 0; i--) {
        if (key < records[i].key) {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;

          if (flush) {
            uint64_t records_ptr = (uint64_t)(&records[i + 1]);

            int remainder = records_ptr % CACHE_LINE_SIZE;
            bool do_flush =
                (remainder == 0) ||
                ((((int)(remainder + sizeof(nvmentry)) / CACHE_LINE_SIZE) == 1) &&
                  ((remainder + sizeof(nvmentry)) % CACHE_LINE_SIZE) != 0);
            if (do_flush) {
              pmemobj_persist(pop, (void *)records_ptr, CACHE_LINE_SIZE);
              to_flush_cnt = 0;
            } else
              ++to_flush_cnt;
          }
        } else {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;

          if (flush)
            pmemobj_persist(pop, &records[i + 1], sizeof(nvmentry));
          inserted = 1;
          break;
        }
      }
      if (inserted == 0) {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;

        if (flush)
          pmemobj_persist(pop, &records[0], sizeof(nvmentry));
      }
    }

    if (update_last_index) {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }


  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max,
                            unsigned long *buf) {
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

      current = D_RW(current->hdr.sibling_ptr);
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
                printf("i: %d last_index: %d key: %ld  ret: %x\n", i , hdr.last_index, key, ret);
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            printf("i: %d last_index: %d key: %ld  ret: %x\n", i , hdr.last_index, key, ret);
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

    printf("%x ", (uint64_t)hdr.sibling_ptr.oid.off);

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
    friend class nvmpage;
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
