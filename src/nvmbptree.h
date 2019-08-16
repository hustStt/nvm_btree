#pragma once

#include <algorithm>
#include <math.h>
#include <string>
#include <iostream>

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

using namespace std;


#define NODESIZE 256

#define CPU_FREQ_MHZ (1994)
#define DELAY_IN_NS (1000)
#define CACHE_LINE_SIZE 64 
#define QUERY_NUM 25

#define IS_FORWARD(c) (c % 2 == 0)

using entry_key_t = uint64_t;


class bpnode;

class btree{
  private:
    int height;
    char* root;
    
  public:
    btree();
    ~btree();
    char *AllocNode();
    void setNewRoot(char *);
    void btree_insert(entry_key_t, char *value);
    void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
    void btree_delete(entry_key_t);
    void btree_delete_internal(entry_key_t, char *, uint32_t, entry_key_t *, bool *, bpnode **);
    char *btree_search(entry_key_t);
    void btree_search_range(entry_key_t, entry_key_t, std::vector<std::string> &values, int &size); 
    void printAll();
    void PrintInfo();

    void PrintStorage(void) {
        printf("Node: \n");
        node_alloc->PrintStorage();
        printf("Value: \n");
        value_alloc->PrintStorage();
    }

    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    friend class bpnode;
};

class header{
  private:
    bpnode* leftmost_ptr;         // 8 bytes
    bpnode* sibling_ptr;          // 8 bytes
    uint32_t level;             // 4 bytes
    uint8_t switch_counter;     // 1 bytes
    uint8_t is_deleted;         // 1 bytes
    int16_t last_index;         // 2 bytes
    std::mutex *mtx;      // 8 bytes

    friend class bpnode;
    friend class btree;

  public:
    header() {
        mtx = new std::mutex();

        leftmost_ptr = NULL;  
        sibling_ptr = NULL;
        switch_counter = 0;
        last_index = -1;
        is_deleted = false;
    }

    ~header() {
        delete mtx;
    }

    void Set_leftmost(bpnode *leftmost_ptr_) {
        pmem_memcpy_persist(&leftmost_ptr, &leftmost_ptr_, sizeof(bpnode *));
    }

    void Set_sibling(bpnode *sibling_ptr_) {
        pmem_memcpy_persist(&sibling_ptr, &sibling_ptr_, sizeof(bpnode *));
    }

    void Set_level(uint32_t level_) {
        pmem_memcpy_persist(&level, &level_, sizeof(uint32_t));
    }

    void Set_switch_counter(uint8_t switch_counter_) {
        pmem_memcpy_persist(&switch_counter, &switch_counter_, sizeof(uint8_t));
    }

    void Set_deleted(uint8_t is_deleted_) {
        pmem_memcpy_persist(&is_deleted, &is_deleted_, sizeof(uint32_t));
    }

    void Set_last_index(int16_t last_index_) {
        pmem_memcpy_persist(&last_index, &last_index_, sizeof(int16_t));
    }

};

class entry{ 
  private:
    entry_key_t key; // 8 bytes
    char* ptr; // 8 bytes

  public:
    entry(){
      key = LONG_MAX;
      ptr = NULL;
    }
    void SetKey(entry_key_t key_) {
        pmem_memcpy_persist(&key, &key_, sizeof(entry_key_t));
    }

    void SetPtr(char* ptr_) {
        pmem_memcpy_persist(&ptr, &ptr_, sizeof(char*));
    }

    friend class bpnode;
    friend class btree;
};

const int cardinality = (NODESIZE-sizeof(header))/sizeof(entry);
const int reserved = NODESIZE - cardinality * sizeof(entry) - sizeof(header);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class bpnode{
  private:
    header hdr;  // header in persistent memory, 16 bytes
    entry records[cardinality]; // slots in persistent memory, 16 bytes * n
    uint8_t reserve[reserved];

  public:
    friend class btree;

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

        // clflush((char*)this, sizeof(bpnode));
        pmem_persist((char*)this, sizeof(bpnode));
    }

    void Set_Key(int i, entry_key_t key_) {
        records[i].SetKey(key_);
    }

    void Set_Ptr(int i, char* ptr_) {
        records[i].SetPtr(ptr_);
    }

    void Persistent() {
        pmem_persist(this, sizeof(bpnode));
    }

    bool remove(btree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true);
    bool remove_rebalancing(btree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true);
    bpnode *store(btree* bt, char* left, entry_key_t key, char* right,
                bool flush, bool with_lock, bpnode *invalid_sibling = NULL);
    char *linear_search(entry_key_t key);
    void linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::string> &values, int &size);
    void print();
    void printAll();

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
        if(IS_FORWARD(hdr.switch_counter)) {
            hdr.Set_switch_counter(hdr.switch_counter + 1);
            // ++hdr.switch_counter;
        }

        bool shift = false;
        int i;
        for(i = 0; records[i].ptr != NULL; ++i) {
            if(!shift && records[i].key == key) {
                Set_Ptr(i, (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr);
                // records[i].ptr = (i == 0) ? 
                // (char *)hdr.leftmost_ptr : records[i - 1].ptr; 
                // Set_Ptr(i, (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr);
                shift = true;
            }

            if(shift) {
                Set_Key(i, records[i + 1].key);
                Set_Ptr(i, records[i + 1].ptr);
                // records[i].key = records[i + 1].key;
                // records[i].ptr = records[i + 1].ptr;

                // // flush
                // uint64_t records_ptr = (uint64_t)(&records[i]);
                // int remainder = records_ptr % CACHE_LINE_SIZE;
                // bool do_flush = (remainder == 0) || 
                //     ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) && 
                //         ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                // if(do_flush) {
                //     clflush((char *)records_ptr, CACHE_LINE_SIZE);
                // }
            }
        }

        if(shift) {
            --hdr.last_index;
        }
        return shift;
    }

    inline void insert_key(entry_key_t key, char* ptr, int *num_entries, bool flush = true,
          bool update_last_index = true) {
        // update switch_counter
        if(!IS_FORWARD(hdr.switch_counter)) {
            hdr.Set_switch_counter(hdr.switch_counter + 1);
            // ++hdr.switch_counter;
        }

        // FAST
        if(*num_entries == 0) {  // this bpnode is empty
            Set_Key(0, key);
            Set_Ptr(0, ptr);
            Set_Ptr(1, NULL);
            // entry* new_entry = (entry*) &records[0];
            // entry* array_end = (entry*) &records[1];
            // new_entry->key = (entry_key_t) key;
            // new_entry->ptr = (char*) ptr;

            // array_end->ptr = (char*)NULL;

            // if(flush) {
            //     clflush((char*) this, CACHE_LINE_SIZE);
            // }
        }
        else {
            int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
            Set_Ptr(*num_entries + 1, records[*num_entries].ptr);
            // records[*num_entries+1].ptr = records[*num_entries].ptr; 
            // if(flush) {
            //     if((uint64_t)&(records[*num_entries+1].ptr) % CACHE_LINE_SIZE == 0) 
            //     clflush((char*)&(records[*num_entries+1].ptr), sizeof(char*));
            // }

            // FAST
            for(i = *num_entries - 1; i >= 0; i--) {
                if(key < records[i].key ) {
                    Set_Ptr(i + 1, records[i].ptr);
                    Set_Key(i + 1, records[i].key);

                    // records[i+1].ptr = records[i].ptr;
                    // records[i+1].key = records[i].key;

                    // if(flush) {
                    //     uint64_t records_ptr = (uint64_t)(&records[i+1]);

                    //     int remainder = records_ptr % CACHE_LINE_SIZE;
                    //     bool do_flush = (remainder == 0) || 
                    //     ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) 
                    //     && ((remainder+sizeof(entry))%CACHE_LINE_SIZE)!=0);
                    //     if(do_flush) {
                    //         clflush((char*)records_ptr,CACHE_LINE_SIZE);
                    //         to_flush_cnt = 0;
                    //     }
                    //     else
                    //         ++to_flush_cnt;
                    // }
                }
                else{
                    Set_Ptr(i + 1, records[i].ptr);
                    Set_Key(i + 1, key);
                    Set_Ptr(i + 1, ptr);
                    // records[i+1].ptr = records[i].ptr;
                    // records[i+1].key = key;
                    // records[i+1].ptr = ptr;

                    // if(flush)
                    //     clflush((char*)&records[i+1],sizeof(entry));
                    inserted = 1;
                    break;
                }
            }
            if(inserted==0){
                Set_Ptr(0, (char *)(hdr.leftmost_ptr));
                Set_Key(0, key);
                Set_Ptr(0, ptr);
                // records[0].ptr =(char*) hdr.leftmost_ptr;
                // records[0].key = key;
                // records[0].ptr = ptr;
                // if(flush)
                //     clflush((char*) &records[0], sizeof(entry)); 
            }
        }

        if(update_last_index) {
            hdr.last_index = *num_entries;
        }
        ++(*num_entries);
    }
};