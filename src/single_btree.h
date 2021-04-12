/*
   Copyright (c) 2018, UNIST. All rights reserved. The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST. 
*/
#pragma once 

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <cassert>
#include <climits>
#include <future>
#include <mutex>

#include "nvm_common.h"
#include "single_pmdk.h"
#include "statistic.h"

//#define PAGESIZE 256

// #define CPU_FREQ_MHZ (1994)
// #define DELAY_IN_NS (1000)
#define CACHE_LINE_SIZE 64 
// #define QUERY_NUM 25

#define IS_FORWARD(c) (c % 2 == 0)

//using entry_key_t = uint64_t;


using namespace std;


class bpnode;


class btree{
  private:
    int height;
    char* root;
    nvmpage* nvm_root;
    uint32_t tar_level;
    uint64_t total_size;
    uint64_t log_off;
    LogAllocator* log_alloc;
    bool flag;
    bool flag2;
    bpnode* it;

  public:
    PMEMobjpool *pop;
    btree(PMEMobjpool *pool);
    btree(PMEMobjpool *pool, uint32_t level);
    btree(bpnode *root);
    void setNewRoot(char *);
    void setLeftmostPtr(bpnode *);
    void btreeInsert(entry_key_t, char*);
    void btreeUpdate(entry_key_t, char*);
    void btree_insert(entry_key_t, char*);
    void btree_update(entry_key_t, char*);
    void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
    void btreeDelete(entry_key_t);
    void btree_delete(entry_key_t);
    void btree_delete_internal(entry_key_t, char *, uint32_t, entry_key_t *, bool *, bpnode **);
    char *btreeSearch(entry_key_t);
    char *btree_search(entry_key_t);
    void btree_search_range(entry_key_t, entry_key_t, unsigned long *); 
    void btree_search_range(entry_key_t, entry_key_t, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size); 
    void btreeSearchRange(entry_key_t, entry_key_t, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size); 
    void btree_search_range(entry_key_t, entry_key_t, std::vector<std::string> & results, int &size); 
    void btreeSearchRange(entry_key_t, entry_key_t, std::vector<std::string> & results, int &size); 
    void btree_search_range(entry_key_t, entry_key_t, void **values, int &size); 
    void btreeSearchRange(entry_key_t , entry_key_t , void **values, int &size);
    void printAll();
    void PrintInfo();
    void CalculateSapce(uint64_t &space);
    void deform();
    void CalcuRootLevel();
    void scan_all_leaf();
    void seq_read(std::vector<std::string> &values, int &size);

    void seak_to_first();
    void* get_next_ptr();

    char* findSubtreeRoot(entry_key_t);
    void to_nvm();
    void to_dram();
    char* DFS(char* root);
    void setFlag(bool flag) {
      this->flag = flag;
    }
    void setFlag2(bool flag) {
      this->flag2 = flag;
    }
    char* getRoot() {
      return root;
    }

    friend class bpnode;
};

class header{
  private:
    bpnode* leftmost_ptr;         // 8 bytes
    bpnode* sibling_ptr;          // 8 bytes
    uint32_t level;               // 4 bytes
    uint8_t switch_counter;       // 1 bytes
    uint8_t status;               // 1 bytes  0:脏节点  1:已删除   2:需要遍历的未修改节点  3:未修改
    int16_t last_index;           // 2 bytes
    uint64_t nvmpage_off;         // 8 bytes

    friend class bpnode;
    friend class nvmpage;
    friend class btree;
    friend class subtree;

  public:
    header() {
      leftmost_ptr = NULL;  
      sibling_ptr = NULL;
      switch_counter = 0;
      last_index = -1;
      status = 0;
      nvmpage_off = -1;
    }

    ~header() {
    }
};

class entry{ 
  private:
    entry_key_t key; // 8 bytes
    char* ptr; // 8 bytes
  public :
    entry(){
      key = LONG_MAX;
      ptr = NULL;
    }

    friend class bpnode;
    friend class nvmpage;
    friend class btree;
    friend class subtree;
};

const int cardinality = (PAGESIZE-sizeof(header))/sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class bpnode{
  private:
    header hdr;  // header in persistent memory, 16 bytes
    entry records[cardinality]; // slots in persistent memory, 16 bytes * n

  public:
    friend class btree;
    friend class subtree;
    friend class nvmpage;

    bpnode(uint32_t level = 0) {
      hdr.level = level;
      records[0].ptr = NULL;
    }

    // this is called when tree grows
    bpnode(bpnode* left, entry_key_t key, bpnode* right, uint32_t level = 0) {
      hdr.leftmost_ptr = left;  
      hdr.level = level;
      records[0].key = key;
      records[0].ptr = (char*) right;
      records[1].ptr = NULL;

      hdr.last_index = 0;
    }
/*
    void *operator new(size_t size) {
      void *ret;
    //   posix_memalign(&ret,64,size);
      alloc_memalign(&ret, 64, size);
      return ret;
    }
*/
    uint32_t GetLevel() {
      return hdr.level;
    }

    void linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::string> &values, int &size, uint64_t base = 0);
    void linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size, uint64_t base = 0);
    void linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size, uint64_t base = 0);


    inline int count() {
      uint8_t previous_switch_counter;
      int count = 0;
      do {
        previous_switch_counter = hdr.switch_counter;
        count = hdr.last_index + 1;

        while(count >= 0 && records[count].ptr != NULL) {
          if(IS_FORWARD(previous_switch_counter))
            ++count;
          else
            --count;
        } 

        if(count < 0) {
          count = 0;
          while(records[count].ptr != NULL) {
            ++count;
          }
        }

      } while(previous_switch_counter != hdr.switch_counter);

      return count;
    }

    inline bool remove_key(entry_key_t key) {
      // Set the switch_counter
      if(IS_FORWARD(hdr.switch_counter)) 
        ++hdr.switch_counter;

      bool shift = false;
      int i;
      for(i = 0; records[i].ptr != NULL; ++i) {
        if(!shift && records[i].key == key) {
          records[i].ptr = (i == 0) ? 
            (char *)hdr.leftmost_ptr : records[i - 1].ptr; 
          shift = true;
        }

        if(shift) {
          records[i].key = records[i + 1].key;
          records[i].ptr = records[i + 1].ptr;

          // sbh modify
          // clflush((char *)(&records[i]), sizeof(entry));
          // flush
          uint64_t records_ptr = (uint64_t)(&records[i]);
          int remainder = records_ptr % CACHE_LINE_SIZE;
          bool do_flush = (remainder == 0) || 
            ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) && 
             ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
        }
      }

      if(shift) {
        --hdr.last_index;
        hdr.status = 0;
      }
      return shift;
    }

    bool remove(btree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true, subtree* sub_root = NULL);
    bool merge(btree *bt, nvmpage *left_sibling, entry_key_t deleted_key_from_parent, subtree* sub_root, subtree* left_subtree_sibling);

    void insert_key(entry_key_t key, char* ptr, int *num_entries);
    bool update_key(entry_key_t key, char* ptr);

    // Insert a new key - FAST and FAIR
    bpnode *store(btree* bt, char* left, entry_key_t key, char* right,
       subtree* sub_root = NULL, bpnode *invalid_sibling = NULL);

    // Search keys with linear search
    void linear_search_range
      (entry_key_t min, entry_key_t max, unsigned long *buf, uint64_t base = 0) {
        int i, off = 0;
        uint8_t previous_switch_counter;
        bpnode *current = this;

        while(current) {
          int old_off = off;
          do {
            previous_switch_counter = current->hdr.switch_counter;
            off = old_off;

            entry_key_t tmp_key;
            char *tmp_ptr;

            if(IS_FORWARD(previous_switch_counter)) {
              if((tmp_key = current->records[0].key) > min) {
                if(tmp_key < max) {
                  if((tmp_ptr = current->records[0].ptr) != NULL) {
                    if(tmp_key == current->records[0].key) {
                      if(tmp_ptr) {
                        buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                }
                else
                  return;
              }

              for(i=1; current->records[i].ptr != NULL; ++i) { 
                if((tmp_key = current->records[i].key) > min) {
                  if(tmp_key < max) {
                    if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                      if(tmp_key == current->records[i].key) {
                        if(tmp_ptr)
                          buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                  else
                    return;
                }
              }
            }
            else {
              for(i=count() - 1; i > 0; --i) { 
                if((tmp_key = current->records[i].key) > min) {
                  if(tmp_key < max) {
                    if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                      if(tmp_key == current->records[i].key) {
                        if(tmp_ptr)
                          buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                  else
                    return;
                }
              }

              if((tmp_key = current->records[0].key) > min) {
                if(tmp_key < max) {
                  if((tmp_ptr = current->records[0].ptr) != NULL) {
                    if(tmp_key == current->records[0].key) {
                      if(tmp_ptr) {
                        buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                }
                else
                  return;
              }
            }
          } while(previous_switch_counter != current->hdr.switch_counter);

          if (IS_VALID_PTR(current->hdr.sibling_ptr) || base == 0) {
            current = current->hdr.sibling_ptr;
          } else {
            current = (bpnode *)((uint64_t)current->hdr.sibling_ptr + base);
          }
        }
      }

    char *linear_search(entry_key_t key) {
      int i = 1;
      uint8_t previous_switch_counter;
      char *ret = NULL;
      char *t; 
      entry_key_t k;

      if(hdr.leftmost_ptr == NULL) { // Search a leaf node
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          // search from left ro right
          if(IS_FORWARD(previous_switch_counter)) { 
            if((k = records[0].key) == key) {
              if((t = records[0].ptr) != NULL) {
                if(k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }

            for(i=1; records[i].ptr != NULL; ++i) { 
              if((k = records[i].key) == key) {
                if(records[i-1].ptr != (t = records[i].ptr)) {
                  if(k == records[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
          else { // search from right to left
            for(i = count() - 1; i > 0; --i) {
              if((k = records[i].key) == key) {
                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                  if(k == records[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }

            if(!ret) {
              if((k = records[0].key) == key) {
                if(NULL != (t = records[0].ptr) && t) {
                  if(k == records[0].key) {
                    ret = t;
                    continue;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if(ret) {
          return ret;
        }
/*
        if((t = (char *)hdr.sibling_ptr) && key >= ((bpnode *)t)->records[0].key)
          return t;
*/
        return NULL;
      }
      else { // internal node
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(previous_switch_counter)) {
            if(key < (k = records[0].key)) {
              if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) { 
                ret = t;
                continue;
              }
            }

            for(i = 1; records[i].ptr != NULL; ++i) { 
              if(key < (k = records[i].key)) { 
                if((t = records[i-1].ptr) != records[i].ptr) {
                  ret = t;
                  break;
                }
              }
            }

            if(!ret) {
              ret = records[i - 1].ptr; 
              continue;
            }
          }
          else { // search from right to left
            for(i = count() - 1; i >= 0; --i) {
              if(key >= (k = records[i].key)) {
                if(i == 0) {
                  if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
                else {
                  if(records[i - 1].ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);
/*
        if((t = (char *)hdr.sibling_ptr) != NULL) {
          if(key >= ((bpnode *)t)->records[0].key)
            return t;
        }
*/
        if(ret) {
          return ret;
        }
        else
          return (char *)hdr.leftmost_ptr;
      }

      return NULL;
    }

    // print a node 
    void print() {
      if(hdr.leftmost_ptr == NULL) 
        printf("[%d] leaf %x \n", this->hdr.level, this);
      else 
        printf("[%d] internal %x \n", this->hdr.level, this);
      printf("last_index: %d\n", hdr.last_index);
      printf("switch_counter: %d\n", hdr.switch_counter);
      printf("search direction: ");
      if(IS_FORWARD(hdr.switch_counter))
        printf("->\n");
      else
        printf("<-\n");

      if(hdr.leftmost_ptr!=NULL) 
        printf("%x ",hdr.leftmost_ptr);

      for(int i=0;records[i].ptr != NULL;++i)
        printf("%ld,%x ",records[i].key,records[i].ptr);

      printf("%x ", hdr.sibling_ptr);

      printf("\n");
    }

    void printAll() {
      if(hdr.leftmost_ptr==NULL) {
        printf("printing leaf node: ");
        print();
      }
      else {
        printf("printing internal node: ");
        print();
        ((bpnode*) hdr.leftmost_ptr)->printAll();
        for(int i=0;records[i].ptr != NULL;++i){
          ((bpnode*) records[i].ptr)->printAll();
        }
      }
    }

    void CalculateSapce(uint64_t &space) {
      if(hdr.leftmost_ptr==NULL) {
        space += PAGESIZE;
      }
      else {
        space += PAGESIZE;
        ((bpnode*) hdr.leftmost_ptr)->CalculateSapce(space);
        for(int i=0;records[i].ptr != NULL;++i){
          ((bpnode*) records[i].ptr)->CalculateSapce(space);
        }
      }
    }
};

static inline bpnode* NewBpNode() {
    return new bpnode();
}
