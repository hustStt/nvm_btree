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

class nvmbtree;
class nvmpage;
class subtree;
class btree;

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_ROOT(btree, nvmbtree);
POBJ_LAYOUT_TOID(btree, nvmpage);
POBJ_LAYOUT_TOID(btree, subtree);
POBJ_LAYOUT_END(btree);

using entry_key_t = int64_t;

using namespace std;

class nvmbtree {
private:
  int height;
  TOID(nvmpage) root;
  

public:
  PMEMobjpool *pop;
  nvmbtree();
  void constructor(PMEMobjpool *);
  void setPMEMobjpool(PMEMobjpool * pop) {
    this->pop = pop;
  }
  void setNewRoot(TOID(nvmpage));
  void btree_insert(entry_key_t, char *);
  void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
  void btree_delete(entry_key_t);
  void btree_delete_internal(entry_key_t, char *, uint32_t, entry_key_t *,
                             bool *, nvmpage **);
  char *btree_search(entry_key_t);
  void btree_search_range(entry_key_t, entry_key_t, unsigned long *);
  void printAll();
  void randScounter();

  friend class nvmpage;
};

class nvmheader {
private:
  TOID(nvmpage) sibling_ptr; // 16 bytes
  nvmpage *leftmost_ptr;     // 8 bytes
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes

  friend class nvmpage;
  friend class nvmbtree;
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
  friend class nvmbtree;
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
  friend class nvmbtree;
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
              bool with_lock = true, subtree* sub_root = NULL) {
    if (!only_rebalance) {
      register int num_entries_before = count();

      // This node is root
      // if (this == D_RO(bt->root)) {
      //   if (hdr.level > 0) {
      //     if (num_entries_before == 1 && (hdr.sibling_ptr.oid.off == 0)) {
      //       bt->root.oid.off = (uint64_t)hdr.leftmost_ptr;
      //       pmemobj_persist(bt->pop, &(bt->root), sizeof(TOID(nvmpage)));

      //       hdr.is_deleted = 1;
      //     }
      //   }

      //   // Remove the key from this node
      //   bool ret = remove_key(bt->pop, key);
      //   return true;
      // }

      bool should_rebalance = true;
      // check the node utilization
      if (num_entries_before - 1 >= (int)((nvm_cardinality - 1) * 0.5)) {
        should_rebalance = false;
      }

      // Remove the key from this node
      bool ret = remove_key(bt->pop, key);

      if (!should_rebalance) {
        return (hdr.leftmost_ptr == NULL) ? ret : true;
      }
    }

    // Remove a key from the parent node
    entry_key_t deleted_key_from_parent = 0;
    bool is_leftmost_node = false;
    TOID(nvmpage) left_sibling;
    left_sibling.oid.pool_uuid_lo = bt->root.oid.pool_uuid_lo;
    subtree * left_subtree_sibling;
    nvmpage * nvm_root = sub_root->get_nvmroot_ptr();

    if (sub_root != NULL && hdr.level == nvm_root>hdr.level) { // subtree root
        bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
          &deleted_key_from_parent, &is_leftmost_node, &left_subtree_sibling);
        left_sibling.oid.off = (uint64_t)left_subtree_sibling->nvm_ptr;
    } else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
        sub_root->btree_delete_internal(key, (char *)pmemobj_oid(this).off, hdr.level + 1,
          &deleted_key_from_parent, &is_leftmost_node, (nvmpage **)&left_sibling.oid.off, bt);
    } else {
        printf("remove error\n");
    }

    if (is_leftmost_node) {
      D_RW(hdr.sibling_ptr)
          ->remove(bt, D_RW(hdr.sibling_ptr)->records[0].key, true, with_lock);
      return true;
    }

    register int num_entries = count();
    register int left_num_entries = D_RW(left_sibling)->count();

    // Merge or Redistribution
    int total_num_entries = num_entries + left_num_entries;
    if (hdr.leftmost_ptr)
      ++total_num_entries;

    entry_key_t parent_key;

    if (total_num_entries > nvm_cardinality - 1) { // Redistribution
      register int m = (int)ceil(total_num_entries / 2);

      if (num_entries < left_num_entries) { // left -> right
        if (hdr.leftmost_ptr == nullptr) {
          for (int i = left_num_entries - 1; i >= m; i--) {
            insert_key(bt->pop, D_RW(left_sibling)->records[i].key,
                       D_RW(left_sibling)->records[i].ptr, &num_entries);
          }

          D_RW(left_sibling)->records[m].ptr = nullptr;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->records[m].ptr),
                          sizeof(char *));

          D_RW(left_sibling)->hdr.last_index = m - 1;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.last_index),
                          sizeof(int16_t));

          parent_key = records[0].key;
        } else {
          insert_key(bt->pop, deleted_key_from_parent, (char *)hdr.leftmost_ptr,
                     &num_entries);

          for (int i = left_num_entries - 1; i > m; i--) {
            insert_key(bt->pop, D_RO(left_sibling)->records[i].key,
                       D_RO(left_sibling)->records[i].ptr, &num_entries);
          }

          parent_key = D_RO(left_sibling)->records[m].key;

          hdr.leftmost_ptr = (nvmpage *)D_RO(left_sibling)->records[m].ptr;
          pmemobj_persist(bt->pop, &(hdr.leftmost_ptr), sizeof(nvmpage *));

          D_RW(left_sibling)->records[m].ptr = nullptr;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->records[m].ptr),
                          sizeof(char *));

          D_RW(left_sibling)->hdr.last_index = m - 1;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.last_index),
                          sizeof(int16_t));
        }

        if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
            bt->btree_insert_internal
              ((char *)left_sibling.oid.off, parent_key, (char *)sub_root, hdr.level + 1);
        }
        else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
            sub_root->btree_insert_internal
              ((char *)left_sibling.oid.off, parent_key, (char *)pmemobj_oid(this).off, hdr.level + 1, bt);
        } else {
            printf("Redistribution left --> right error\n");
        }

        // if (left_sibling.oid.off == bt->root.oid.off) {
        //   TOID(nvmpage) new_root;
        //   POBJ_NEW(bt->pop, &new_root, nvmpage, NULL, NULL);
        //   D_RW(new_root)->constructor(bt->pop, (nvmpage *)left_sibling.oid.off,
        //                               parent_key, (nvmpage *)pmemobj_oid(this).off,
        //                               hdr.level + 1);
        //   bt->setNewRoot(new_root);
        // } else {
        //   bt->btree_insert_internal((char *)left_sibling.oid.off, parent_key,
        //                             (char *)pmemobj_oid(this).off,
        //                             hdr.level + 1);
        // }
      } else { // from leftmost case
        hdr.is_deleted = 1;
        pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

        TOID(nvmpage) new_sibling;
        POBJ_NEW(bt->pop, &new_sibling, nvmpage, NULL, NULL);
        D_RW(new_sibling)->constructor(hdr.level);
        D_RW(new_sibling)->hdr.sibling_ptr = hdr.sibling_ptr;

        int num_dist_entries = num_entries - m;
        int new_sibling_cnt = 0;

        if (hdr.leftmost_ptr == nullptr) {
          for (int i = 0; i < num_dist_entries; i++) {
            D_RW(left_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &left_num_entries);
          }

          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            D_RW(new_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &new_sibling_cnt, false);
          }

          pmemobj_persist(bt->pop, D_RW(new_sibling), sizeof(nvmpage));

          D_RW(left_sibling)->hdr.sibling_ptr = new_sibling;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.sibling_ptr),
                          sizeof(nvmpage *));

          parent_key = D_RO(new_sibling)->records[0].key;
        } else {
          D_RW(left_sibling)
              ->insert_key(bt->pop, deleted_key_from_parent,
                           (char *)hdr.leftmost_ptr, &left_num_entries);

          for (int i = 0; i < num_dist_entries - 1; i++) {
            D_RW(left_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &left_num_entries);
          }

          parent_key = records[num_dist_entries - 1].key;

          D_RW(new_sibling)->hdr.leftmost_ptr =
              (nvmpage *)records[num_dist_entries - 1].ptr;
          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            D_RW(new_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &new_sibling_cnt, false);
          }
          pmemobj_persist(bt->pop, D_RW(new_sibling), sizeof(nvmpage));

          D_RW(left_sibling)->hdr.sibling_ptr = new_sibling;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.sibling_ptr),
                          sizeof(nvmpage *));
        }

        if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
            sub_root->nvm_ptr = (nvmpage *)new_sibling.oid.off;
            pmemobj_persist(bt->pop, sub_root, sizeof(subtree));

            bt->btree_insert_internal
              ((char *)eft_sibling.oid.off, parent_key, (char *)sub_root, hdr.level + 1);
        }
        else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
            sub_root->btree_insert_internal
              ((char *)left_sibling.oid.off, parent_key, (char *)new_sibling.oid.off, hdr.level + 1, bt);
        }

        // if (left_sibling.oid.off == bt->root.oid.off) {
        //   TOID(nvmpage) new_root;
        //   POBJ_NEW(bt->pop, &new_root, nvmpage, NULL, NULL);
        //   D_RW(new_root)->constructor(bt->pop, (nvmpage *)left_sibling.oid.off,
        //                               parent_key, (nvmpage *)new_sibling.oid.off,
        //                               hdr.level + 1);
        //   bt->setNewRoot(new_root);
        // } else {
        //   bt->btree_insert_internal((char *)left_sibling.oid.off, parent_key,
        //                             (char *)new_sibling.oid.off, hdr.level + 1);
        // }
      }
    } else {
      hdr.is_deleted = 1;
      pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

      if (hdr.leftmost_ptr)
        D_RW(left_sibling)
            ->insert_key(bt->pop, deleted_key_from_parent,
                         (char *)hdr.leftmost_ptr, &left_num_entries);

      for (int i = 0; records[i].ptr != NULL; ++i) {
        D_RW(left_sibling)
            ->insert_key(bt->pop, records[i].key, records[i].ptr,
                         &left_num_entries);
      }

      D_RW(left_sibling)->hdr.sibling_ptr = hdr.sibling_ptr;
      pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.sibling_ptr),
                      sizeof(nvmpage *));

      // subtree root
      if (sub_root != NULL && hdr.level == nvm_root->hdr.level) {
        //delete sub_root
        left_subtree_sibling->sibling_ptr = sub_root->sibling_ptr;
        pmemobj_persist(bt->pop, left_subtree_sibling, sizeof(subtree));
      }
    }

    return true;
  }

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

  // Insert a new key - FAST and FAIR
  nvmpage *store(btree *bt = NULL, char *left, entry_key_t key, char *right, bool flush,
              subtree *sub_root, nvmpage *invalid_sibling = NULL) {
    // If this node has a sibling node,
    // if ((hdr.sibling_ptr.oid.off != 0) &&
    //     ((nvmpage *)hdr.sibling_ptr.oid.off != invalid_sibling)) {
    //   // Compare this key with the first key of the sibling
    //   if (key > D_RO(hdr.sibling_ptr)->records[0].key) {
    //     return D_RW(hdr.sibling_ptr)
    //         ->store(bt, NULL, key, right, true, invalid_sibling);
    //   }
    // }

    register int num_entries = count();

    // FAST
    if (num_entries < nvm_cardinality - 1) {
      insert_key(bt->pop, key, right, &num_entries, flush);
      return (nvmpage *)pmemobj_oid(this).off;
    } else { // FAIR
      // overflow
      // create a new node
      TOID(nvmpage) sibling;
      POBJ_NEW(bt->pop, &sibling, nvmpage, NULL, NULL);
      D_RW(sibling)->constructor(hdr.level);
      nvmpage *sibling_ptr = D_RW(sibling);
      register int m = (int)ceil(num_entries / 2);
      entry_key_t split_key = records[m].key;

      // migrate half of keys into the sibling
      int sibling_cnt = 0;
      if (hdr.leftmost_ptr == NULL) { // leaf node
        for (int i = m; i < num_entries; ++i) {
          sibling_ptr->insert_key(bt->pop, records[i].key, records[i].ptr,
                                  &sibling_cnt, false);
        }
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling_ptr->insert_key(bt->pop, records[i].key, records[i].ptr,
                                  &sibling_cnt, false);
        }
        sibling_ptr->hdr.leftmost_ptr = (nvmpage *)records[m].ptr;
      }

      sibling_ptr->hdr.sibling_ptr = hdr.sibling_ptr;
      pmemobj_persist(bt->pop, sibling_ptr, sizeof(nvmpage));

      hdr.sibling_ptr = sibling;
      pmemobj_persist(bt->pop, &hdr, sizeof(hdr));

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;
      pmemobj_persist(bt->pop, &records[m], sizeof(nvmentry));

      hdr.last_index = m - 1;
      pmemobj_persist(bt->pop, &hdr.last_index, sizeof(int16_t));

      num_entries = hdr.last_index + 1;

      nvmpage *ret;

      // insert the key
      if (key < split_key) {
        insert_key(bt->pop, key, right, &num_entries);
        ret = (nvmpage *)pmemobj_oid(this).off;
      } else {
        sibling_ptr->insert_key(bt->pop, key, right, &sibling_cnt);
        ret = (nvmpage *)sibling.oid.off;
      }

      nvmpage * nvm_root = sub_root->get_nvmroot_ptr();
      
      // Set a new root or insert the split key to the parent
      if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
        subtree* next = newSubtreeRoot(bt->pop, (nvmpage *)sibling.oid.off, sub_root->sibling_ptr);
        sub_root->sibling_ptr = (subtree *)pmemobj_oid(next).off;
        pmemobj_persist(bt->pop, sub_root, sizeof(subtree));

        bt->btree_insert_internal(NULL, split_key, (char *)next, 
            hdr.level + 1);
      }
      else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
        sub_root->btree_insert_internal(NULL, split_key, (char *)sibling.oid.off, 
            hdr.level + 1, bt);
      } 
      else { // internal node
        printf("nvm subtree insert error\n");
      }

      return ret;
    }
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

