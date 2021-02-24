#include "single_pmdk.h"
#include "single_btree.h"

/*
 * class nvmbtree
 */

MyBtree * MyBtree::mybt = nullptr;

bool nvmpage::remove(btree *bt, entry_key_t key, bool only_rebalance,
            bool with_lock, subtree* sub_root) {
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
  nvmpage* left_sibling;
  bpnode* left_dram_sibling;
  //left_sibling.oid.pool_uuid_lo = bt->root.oid.pool_uuid_lo;
  subtree * left_subtree_sibling = sub_root;
  nvmpage * nvm_root = sub_root->get_nvmroot_ptr();

  if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
      bt->btree_delete_internal(key, (char *)sub_root, hdr.level + 1,
        &deleted_key_from_parent, &is_leftmost_node, (bpnode **)&left_sibling);
      left_subtree_sibling = (subtree *)left_sibling;
      //left_sibling = left_subtree_sibling->get_nvmroot_ptr();
      // todo
      if (left_subtree_sibling->isNVMBtree()) {
        left_sibling = left_subtree_sibling->get_nvmroot_ptr();
      } else {
        left_dram_sibling = left_subtree_sibling->dram_ptr;
        if (is_leftmost_node) {
          // merge
          left_dram_sibling->remove(bt, left_dram_sibling->records[0].key, true, with_lock, left_subtree_sibling);
          return true;
        }
        // // 不同介质间的合并操作
        // merge(bt, left_dram_sibling, deleted_key_from_parent ,sub_root, left_subtree_sibling);
        RebalanceTask * rt = new RebalanceTask(left_subtree_sibling, sub_root, nullptr, this, deleted_key_from_parent);
        sub_root->rt = rt;
        return true;
      }
  } else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
      sub_root->btree_delete_internal(key, (char *)pmemobj_oid(this).off, hdr.level + 1,
        &deleted_key_from_parent, &is_leftmost_node, &left_sibling, bt);
  } else {
      printf("remove error\n");
  }

  if (is_leftmost_node) {
    // 此处left_sibling 是当前节点的右兄弟节点
    left_sibling->remove(bt, left_sibling->records[0].key, true, with_lock, left_subtree_sibling);
    return true;
  }

  register int num_entries = count();
  register int left_num_entries = left_sibling->count();

  uint64_t l = left_subtree_sibling->getHeat();
  uint64_t r = sub_root->getHeat();

  // Merge or Redistribution
  int total_num_entries = num_entries + left_num_entries;
  if (hdr.leftmost_ptr != nullptr)
    ++total_num_entries;

  entry_key_t parent_key;

  if (total_num_entries > nvm_cardinality - 1) { // Redistribution
    register int m = (int)ceil(total_num_entries / 2);

    if (num_entries < left_num_entries) { // left -> right
      if (hdr.leftmost_ptr == nullptr) {
        for (int i = left_num_entries - 1; i >= m; i--) {
           insert_key(bt->pop, left_sibling->records[i].key,
                      left_sibling->records[i].ptr, &num_entries);
        }

        left_sibling->records[m].ptr = nullptr;
        pmemobj_persist(bt->pop, &(left_sibling->records[m].ptr),
                        sizeof(char *));

        left_sibling->hdr.last_index = m - 1;
        pmemobj_persist(bt->pop, &(left_sibling->hdr.last_index), 
                        sizeof(int16_t));

        parent_key = records[0].key;
      } else {
        insert_key(bt->pop, deleted_key_from_parent, (char *)hdr.leftmost_ptr,
                    &num_entries);

        for (int i = left_num_entries - 1; i > m; i--) {
          insert_key(bt->pop, left_sibling->records[i].key,
                      left_sibling->records[i].ptr, &num_entries);
        }

        parent_key = left_sibling->records[m].key;

        hdr.leftmost_ptr = (nvmpage *)left_sibling->records[m].ptr;
        pmemobj_persist(bt->pop, &(hdr.leftmost_ptr), sizeof(nvmpage *));

        left_sibling->records[m].ptr = nullptr;
        pmemobj_persist(bt->pop, &(left_sibling->records[m].ptr),
                        sizeof(char *));

        left_sibling->hdr.last_index = m - 1;
        pmemobj_persist(bt->pop, &(left_sibling->hdr.last_index),
                        sizeof(int16_t));
      }

      if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
          bt->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
          // heat  (left_num_entries - m) / left_num_entries 
          sub_root->setHeat(r + (left_num_entries - m) / left_num_entries * l);
          left_subtree_sibling->setHeat(m / left_num_entries * l);
      }
      else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
          sub_root->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)pmemobj_oid(this).off, hdr.level + 1, bt);
      } else {
          printf("Redistribution left --> right error\n");
      }
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
          left_sibling->insert_key(bt->pop, records[i].key, records[i].ptr,
                            &left_num_entries);
        }

        for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
          D_RW(new_sibling)
              ->insert_key(bt->pop, records[i].key, records[i].ptr,
                            &new_sibling_cnt, false);
        }

        pmemobj_persist(bt->pop, D_RW(new_sibling), sizeof(nvmpage));

        left_sibling->hdr.sibling_ptr = (nvmpage *)new_sibling.oid.off;
        pmemobj_persist(bt->pop, &(left_sibling->hdr.sibling_ptr),
                        sizeof(nvmpage *));

        parent_key = D_RO(new_sibling)->records[0].key;
      } else {
        left_sibling->insert_key(bt->pop, deleted_key_from_parent,
                          (char *)hdr.leftmost_ptr, &left_num_entries);

        for (int i = 0; i < num_dist_entries - 1; i++) {
          left_sibling->insert_key(bt->pop, records[i].key, records[i].ptr,
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
      }

      if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
          sub_root->setNewNvmRoot((nvmpage *)new_sibling.oid.off);
          // heat
          sub_root->setHeat(m / num_entries * r);
          left_subtree_sibling->setHeat(l + num_dist_entries / left_num_entries * r);

          bt->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
      }
      else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
          sub_root->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)new_sibling.oid.off, hdr.level + 1, bt);
      } else {
        printf("leftmost case error\n");
      }
    }
  } else {
    hdr.is_deleted = 1;
    pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

    if (hdr.leftmost_ptr)
      left_sibling->insert_key(bt->pop, deleted_key_from_parent,
                        (char *)hdr.leftmost_ptr, &left_num_entries);

    for (int i = 0; records[i].ptr != NULL; ++i) {
      left_sibling->insert_key(bt->pop, records[i].key, records[i].ptr,
                        &left_num_entries);
    }

    if (hdr.leftmost_ptr == nullptr) {
      left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      pmemobj_persist(bt->pop, &(left_sibling->hdr.sibling_ptr),
                      sizeof(nvmpage *));
    }
    //printf("left_sibling del off %p %lx\n", left_sibling, left_sibling->hdr.sibling_ptr.oid.off);

    // subtree root
    if (sub_root != NULL && hdr.level == nvm_root->hdr.level) {
      //delete sub_root
      left_subtree_sibling->setSiblingPtr(sub_root->sibling_ptr);
      if (left_subtree_sibling->getSiblingPtr()) 
        left_subtree_sibling->getSiblingPtr()->setPrePtr((subtree *)pmemobj_oid(left_subtree_sibling).off);
      left_subtree_sibling->setHeat(l + r);
    }
  }

  return true;
}

bool nvmpage::merge(btree *bt, bpnode *left_sibling, entry_key_t deleted_key_from_parent, subtree* sub_root, subtree* left_subtree_sibling) {
  register int num_entries = count();
  register int left_num_entries = left_sibling->count();
  uint64_t l = left_subtree_sibling->getHeat();
  uint64_t r = sub_root->getHeat();

  // Merge or Redistribution
  int total_num_entries = num_entries + left_num_entries;
  if (hdr.leftmost_ptr != nullptr)
    ++total_num_entries;

  entry_key_t parent_key;
  if (hdr.leftmost_ptr == nullptr) {
    printf("error\n");
    return false;
  }

  if (total_num_entries > nvm_cardinality - 1) { // Redistribution
    register int m = (int)ceil(total_num_entries / 2);

    if (num_entries < left_num_entries) { // left -> right
      {
        if (left_sibling->records[m].ptr == nullptr) {
          bt->btree_insert_internal
            ((char *)left_sibling, deleted_key_from_parent, (char *)sub_root, hdr.level + 1);
          return true;
        }
        insert_key(bt->pop, deleted_key_from_parent, (char *)hdr.leftmost_ptr,
                    &num_entries);

        nvmpage * pre = (nvmpage *)left_subtree_sibling->getDramDataNode(left_sibling->records[m-1].ptr);// todo
        hdr.leftmost_ptr = (nvmpage *)sub_root->DFS(left_sibling->records[m].ptr, &pre);
        pmemobj_persist(bt->pop, &(hdr.leftmost_ptr), sizeof(nvmpage *));

        for (int i = m + 1; i < left_num_entries; i++) {
          insert_key(bt->pop, left_sibling->records[i].key,
                      sub_root->DFS(left_sibling->records[i].ptr, &pre), &num_entries);
        }

        parent_key = left_sibling->records[m].key;

        left_sibling->records[m].ptr = nullptr;

        left_sibling->hdr.last_index = m - 1;
      }
      //heat
      sub_root->setHeat(r + (left_num_entries - m) / left_num_entries * l);
      left_subtree_sibling->setHeat(m / left_num_entries * l);

      // log标记合并操作
      //left_subtree_sibling->log_alloc->operateTree(left_sibling->hdr.nvmpage_off, -1, m, 11);

      bt->btree_insert_internal
        ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
    } else { // from leftmost case
      hdr.is_deleted = 1;
      pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

      TOID(nvmpage) new_sibling;
      POBJ_NEW(bt->pop, &new_sibling, nvmpage, NULL, NULL);
      D_RW(new_sibling)->constructor(hdr.level);
      D_RW(new_sibling)->hdr.sibling_ptr = hdr.sibling_ptr;

      int num_dist_entries = num_entries - m;
      int new_sibling_cnt = 0;

      {
        //left_subtree_sibling->log_alloc->writeKv(left_sibling->hdr.nvmpage_off, deleted_key_from_parent, (char *)hdr.leftmost_ptr);
        bpnode * pre = left_subtree_sibling->getLastDDataNode();// todo
        left_sibling->insert_key(deleted_key_from_parent,
                          sub_root->DFS(hdr.leftmost_ptr, &pre), &left_num_entries);

        for (int i = 0; i < num_dist_entries - 1; i++) {
          //left_subtree_sibling->log_alloc->writeKv(left_sibling->hdr.nvmpage_off, records[i].key, records[i].ptr);
          left_sibling->insert_key(records[i].key, sub_root->DFS((nvmpage *)records[i].ptr, &pre),
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
      }

      sub_root->nvm_ptr = (nvmpage *)new_sibling.oid.off;
      pmemobj_persist(bt->pop, sub_root, sizeof(subtree));

      sub_root->setHeat(m / num_entries * r);
      left_subtree_sibling->setHeat(l + num_dist_entries / left_num_entries * r);

      bt->btree_insert_internal
        ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
    }
  } else {
    hdr.is_deleted = 1;
    pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

    //left_subtree_sibling->log_alloc->writeKv(left_sibling->hdr.nvmpage_off, deleted_key_from_parent, (char *)hdr.leftmost_ptr);
    bpnode * pre = left_subtree_sibling->getLastDDataNode();
    left_sibling->insert_key(deleted_key_from_parent,
                        sub_root->DFS(hdr.leftmost_ptr, &pre), &left_num_entries);

    for (int i = 0; records[i].ptr != NULL; ++i) {
      //left_subtree_sibling->log_alloc->writeKv(left_sibling->hdr.nvmpage_off, records[i].key, records[i].ptr);
      left_sibling->insert_key(records[i].key, sub_root->DFS((nvmpage *)records[i].ptr, &pre),
                        &left_num_entries);
    }

    // if (hdr.leftmost_ptr == nullptr) {
    //   left_sibling->hdr.sibling_ptr = (bpnode *)hdr.sibling_ptr;
    // }
    //printf("left_sibling del off %p %lx\n", left_sibling, left_sibling->hdr.sibling_ptr.oid.off);

    // subtree root
      //delete sub_root
    left_subtree_sibling->setSiblingPtr(sub_root->sibling_ptr);
    if (left_subtree_sibling->getSiblingPtr()) 
      left_subtree_sibling->getSiblingPtr()->setPrePtr((subtree *)pmemobj_oid(left_subtree_sibling).off);
    left_subtree_sibling->setHeat(l + r);
  }

  return true;
}

inline bool nvmpage::update_key(PMEMobjpool *pop, entry_key_t key, char* ptr) {
  register int num_entries = count();

  if (num_entries == 0) {
    return false;
  }
  int left = 0;
  int right = num_entries - 1;
  while(left <= right) { // 注意
      int mid = (right + left) / 2;
      if(records[mid].key == key) {
        records[mid].ptr = ptr;
        pmemobj_persist(pop, &records[mid], sizeof(nvmentry));
        return true; 
      } else if (records[mid].key < key) {
          left = mid + 1; // 注意
      } else if (records[mid].key > key) {
          right = mid - 1; // 注意
      }
  }
  return false;
}

inline void nvmpage::insert_key(PMEMobjpool *pop, entry_key_t key, char *ptr,
                      int *num_entries, bool flush,
                      bool update_last_index) {
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
nvmpage *nvmpage::store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
            subtree *sub_root, nvmpage *invalid_sibling) {
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

    hdr.sibling_ptr = (nvmpage *)sibling.oid.off;
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
      subtree* next = newSubtreeRoot(bt->pop, (nvmpage *)sibling.oid.off, sub_root);
      if (sub_root->getSiblingPtr()) 
        sub_root->getSiblingPtr()->setPrePtr((subtree *)pmemobj_oid(next).off);
      sub_root->setSiblingPtr((subtree *)pmemobj_oid(next).off);
      sub_root->setHeat(sub_root->getHeat() / 2);

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

void nvmpage::linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size, uint64_t base) {
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
                  //buf[off++] = (unsigned long)tmp_ptr;
                  // values[off] = tmp_ptr;
                  results.push_back({tmp_key, (uint64_t)tmp_ptr});
                  off++;
                  if(off >= size) {
                    return ;
                  }
                }
              }
            }
          } else {
            size = off;
            return;
          }
        }

        for (i = 1; current->records[i].ptr != NULL; ++i) {
          if ((tmp_key = current->records[i].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[i].ptr) !=
                  current->records[i - 1].ptr) {
                if (tmp_key == current->records[i].key) {
                  if (tmp_ptr) {
                    //buf[off++] = (unsigned long)tmp_ptr;
                    // values[off] = tmp_ptr;
                    results.push_back({tmp_key, (uint64_t)tmp_ptr});
                    off++;
                    if(off >= size) {
                      return ;
                    }
                  }
                }
              }
            } else {
              size = off;
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
                  if (tmp_ptr) {
                    //buf[off++] = (unsigned long)tmp_ptr;
                    // values[off] = tmp_ptr;
                    results.push_back({tmp_key, (uint64_t)tmp_ptr});
                    off++;
                    if(off >= size) {
                        return ;
                    }
                  }
                }
              }
            } else {
              size = off;
              return;
            }
          }
        }

        if ((tmp_key = current->records[0].key) > min) {
          if (tmp_key < max) {
            if ((tmp_ptr = current->records[0].ptr) != NULL) {
              if (tmp_key == current->records[0].key) {
                if (tmp_ptr) {
                  //buf[off++] = (unsigned long)tmp_ptr;
                  // values[off] = tmp_ptr;
                  results.push_back({tmp_key, (uint64_t)tmp_ptr});
                  off++;
                  if(off >= size) {
                      return ;
                  }
                }
              }
            }
          } else {
            size = off;
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

void nvmpage::linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size, uint64_t base) {
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
                  //buf[off++] = (unsigned long)tmp_ptr;
                  values[off] = tmp_ptr;
                  off++;
                  if(off >= size) {
                    return ;
                  }
                }
              }
            }
          } else {
            size = off;
            return;
          }
        }

        for (i = 1; current->records[i].ptr != NULL; ++i) {
          if ((tmp_key = current->records[i].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[i].ptr) !=
                  current->records[i - 1].ptr) {
                if (tmp_key == current->records[i].key) {
                  if (tmp_ptr) {
                    //buf[off++] = (unsigned long)tmp_ptr;
                    values[off] = tmp_ptr;
                    off++;
                    if(off >= size) {
                      return ;
                    }
                  }
                }
              }
            } else {
              size = off;
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
                  if (tmp_ptr) {
                    //buf[off++] = (unsigned long)tmp_ptr;
                    values[off] = tmp_ptr;
                    off++;
                    if(off >= size) {
                        return ;
                    }
                  }
                }
              }
            } else {
              size = off;
              return;
            }
          }
        }

        if ((tmp_key = current->records[0].key) > min) {
          if (tmp_key < max) {
            if ((tmp_ptr = current->records[0].ptr) != NULL) {
              if (tmp_key == current->records[0].key) {
                if (tmp_ptr) {
                  //buf[off++] = (unsigned long)tmp_ptr;
                  values[off] = tmp_ptr;
                  off++;
                  if(off >= size) {
                      return ;
                  }
                }
              }
            }
          } else {
            size = off;
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


void subtree::subtree_insert(btree* root, entry_key_t key, char* right) {
  if (flag) {
    // write log
    log_alloc->writeKv(key, right);
    bpnode *p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL) {
      if(p->hdr.status == 3) p->hdr.status = 2;
      p = (bpnode*)p->linear_search(key);
    }

    if(!p->store(root, NULL, key, right, this)) { // store 
      subtree_insert(root, key, right);
    }
  } else {
    nvmpage* p = get_nvmroot_ptr();

    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(key));
    }

    if (!p->store(root, NULL, key, right, true, this)) { // store
      subtree_insert(root, key, right);
    }
  }
}

void subtree::subtree_update(btree* root, entry_key_t key, char* right) {
  if (flag) {
    // write log
    log_alloc->updateKv(key, right);
    bpnode *p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL) {
      if(p->hdr.status == 3) p->hdr.status = 2;
      p = (bpnode*)p->linear_search(key);
    }
    //log_alloc->updateKv(p->hdr.nvmpage_off, key, right);

    if(!p->update_key(key, right)) { // store 
      // printf("no such key\n");
    }
  } else {
    nvmpage* p = get_nvmroot_ptr();

    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(key));
    }

    if (!p->update_key(pop, key, right)) { // store
      // printf("no such key\n");
    }
  }
}

void subtree::subtree_delete(btree* root, entry_key_t key) {
  if (flag) {
    // write log
    log_alloc->deleteKey(key);
    bpnode* p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL){
      if(p->hdr.status == 3) p->hdr.status = 2;
      p = (bpnode*) p->linear_search(key);
    }

    bpnode *t = (bpnode *)p->linear_search(key);
    if(p && t) {
      if(!p->remove(root, key, false, true, this)) {
        subtree_delete(root, key);
      }
    }
    else {
        ;
      // printf("not found the key to delete %llx\n", key);
    }
  } else {
    nvmpage* p = get_nvmroot_ptr();

    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(key));
    }

    uint64_t t = (uint64_t)p->linear_search(key);
    if (t) {
      if (!p->remove(root, key, false, true, this)) {
        subtree_delete(root, key);
      }
    } else {
      // printf("not found the key to delete %lu\n", key);
    }
  }
}

char* subtree::subtree_search(entry_key_t key) {
  if (flag) {
    bpnode* p = dram_ptr;
    while(p->hdr.leftmost_ptr != NULL) {
      p = (bpnode *)p->linear_search(key);
    }

    bpnode *t = (bpnode *)p->linear_search(key);
    if(!t) {
      // printf("NOT FOUND %llx, t = %x\n", key, t);
      return NULL;
    }

    return (char *)t;
  } else {
    nvmpage* p = get_nvmroot_ptr();
    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(key));
    }

    uint64_t t = (uint64_t)p->linear_search(key);
    if (!t) {
      printf("NOT FOUND %lu, t = %x\n", key, t);
      return NULL;
    }

    return (char *)t;
  }
}

void subtree::nvm_to_dram(bpnode **pre) {
  if (flag) {
    return ;
  }
  flag = true;
  uint64_t start_time, end_time;
  start_time = get_now_micros();
  dram_ptr = (bpnode *)DFS(nvm_ptr, pre);
  end_time = get_now_micros();
  printf("subtree to dram  time: %f s\n", (end_time - start_time) * 1e-6);
  // nvm_ptr = nullptr;
  log_off = getNewLogAllocator();
  log_alloc = node_alloc->getNVMptr(log_off);
}

void subtree::dram_recovery(bpnode **pre) {
  if (flag) {
    return ;
  }
  flag = true;
  uint64_t start_time, end_time;
  start_time = get_now_micros();
  dram_ptr = (bpnode *)DFS(nvm_ptr, pre);
  end_time = get_now_micros();
  printf("subtree to dram  time: %f s\n", (end_time - start_time) * 1e-6);
}

char* subtree::DFS(nvmpage* root, bpnode **pre) {
    if(root == nullptr) {
        return nullptr;
    }
    
    nvmpage* nvm_node_ptr = to_nvmpage(root);
    bpnode* node = new bpnode();
    
    int count = 0;
    
    node->hdr.status = 3; //已经同步 不是脏节点
    node->hdr.last_index = nvm_node_ptr->hdr.last_index;
    node->hdr.level = nvm_node_ptr->hdr.level;
    node->hdr.switch_counter = nvm_node_ptr->hdr.switch_counter;
    node->hdr.nvmpage_off = (uint64_t)root;
    node->hdr.sibling_ptr = (bpnode *)nvm_node_ptr->hdr.sibling_ptr;
    
    node->hdr.leftmost_ptr = (bpnode *)DFS(nvm_node_ptr->hdr.leftmost_ptr, pre);
    while(nvm_node_ptr->records[count].ptr != NULL) {
        node->records[count].key = nvm_node_ptr->records[count].key;
        if (nvm_node_ptr->hdr.leftmost_ptr != nullptr) {
            node->records[count].ptr = DFS((nvmpage *)nvm_node_ptr->records[count].ptr, pre);
        } else {
            node->records[count].ptr = nvm_node_ptr->records[count].ptr;
        }
        ++count;
    }
    node->records[count].ptr = nullptr;

    if (node->hdr.leftmost_ptr == nullptr) {
      if (*pre != nullptr) {
        (*pre)->hdr.sibling_ptr = node;
      }
      *pre = node;
    }

    return (char *)node;
}

void subtree::dram_to_nvm(nvmpage **pre) {
  if (!flag) {
    return ;
  }

  uint64_t start_time, end_time;
  start_time = get_now_micros();
  transing = true;
  nvm_ptr = (nvmpage *)DFS((char *)dram_ptr, pre);
  transing = false;
  end_time = get_now_micros();
  printf("subtree to nvm  time: %f s\n", (end_time - start_time) * 1e-6);
  dram_ptr = nullptr;
  flag = false;
  // delete log
  if (log_alloc) log_alloc->DeleteLog();
  log_off = -1;
  log_alloc = nullptr;
}

void subtree::sync_subtree(nvmpage **pre) {
  if (!flag) {
    return ;
  }
  uint64_t start_time, end_time;
  start_time = get_now_micros();
  transing = true;
  nvm_ptr = (nvmpage *)DFS((char *)dram_ptr, pre, false);
  transing = false;
  end_time = get_now_micros();
  printf("subtree sync  time: %f s\n", (end_time - start_time) * 1e-6);
  // delete log
  if (log_alloc) log_alloc->DeleteLog();
  log_off = getNewLogAllocator();
  log_alloc = node_alloc->getNVMptr(log_off);
}

char* subtree::DFS(char* root, nvmpage **pre, bool ifdel) {
    if(root == nullptr) {
        return nullptr;
    }
    bpnode* node = (bpnode *)root;
    nvmpage *tmp_ptr = new nvmpage();
    memcpy(tmp_ptr, node, sizeof(bpnode));
    nvmpage* nvm_node_ptr;
    TOID(nvmpage) nvm_node;
    char * ret;
    bool isflush = true;

    if (node->hdr.status == 3 && node->hdr.nvmpage_off != -1) {
      // 干净节点 直接返回
      //printf("internal node clear node level %d\n",node->hdr.level);
      // pre
      return (char *)node->hdr.nvmpage_off;  
    } else if (node->hdr.status == 2 && node->hdr.nvmpage_off != -1) {
      // 干净节点 访问过的路径
      isflush = false;
    } else if (node->hdr.status == 1) {
      // 已删除
      printf("error : this node is deleted.\n");
      return nullptr;
    } else if (node->hdr.status == 0){
      // 脏节点
    }
    node->hdr.status = 3;

    if (node->hdr.nvmpage_off != -1) { // 复用nvmpage
      // sibling
      nvm_node_ptr = to_nvmpage((char *)node->hdr.nvmpage_off);
      ret = (char *)node->hdr.nvmpage_off;
    } else { // 新节点 重新申请nvmpage
      POBJ_NEW(pop, &nvm_node, nvmpage, NULL, NULL);
      D_RW(nvm_node)->constructor();
      nvm_node_ptr = D_RW(nvm_node);
      node->hdr.nvmpage_off = nvm_node.oid.off;
      ret = (char *)nvm_node.oid.off;
    }
    
    // 形成新的bpnode
    int count = 0;
    tmp_ptr->hdr.leftmost_ptr = (nvmpage *)DFS((char *)node->hdr.leftmost_ptr, pre, ifdel);
    while(node->records[count].ptr != NULL) {
        if (node->hdr.leftmost_ptr != nullptr) {
            tmp_ptr->records[count].ptr = DFS(node->records[count].ptr, pre, ifdel);
        } 
        ++count;
    }
    tmp_ptr->records[count].ptr = nullptr;

    // 整块刷下去
    if (isflush) {
      memcpy(nvm_node_ptr, tmp_ptr, sizeof(nvmpage));
      pmemobj_persist(pop, nvm_node_ptr, sizeof(nvmpage));
    }

    if (node->hdr.leftmost_ptr == nullptr) {
      if (*pre != nullptr) {
        (*pre)->hdr.sibling_ptr = (nvmpage *)ret;
        //pmemobj_persist(pop, &((*pre)->hdr.sibling_ptr), sizeof(nvmpage *));
      }
      *pre = nvm_node_ptr;
    }

    if (ifdel) delete node;
    delete tmp_ptr;
    return ret;
}


void subtree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level, btree *bt) {
    if (flag) {
        if(level > dram_ptr->hdr.level)
            return;

        bpnode *p = dram_ptr;

        while(p->hdr.level > level) 
            p = (bpnode *)p->linear_search(key);

        if(!p->store(bt, NULL, key, right, this)) {
            btree_insert_internal(left, key, right, level, bt);
        }
    } else {
        nvmpage* root = get_nvmroot_ptr();
        if (level > root->hdr.level)
            return;

        nvmpage* p = root;
        while (p->hdr.level > level)
            p = to_nvmpage(p->linear_search(key));

        if (!p->store(bt, NULL, key, right, true, this)) {
            btree_insert_internal(left, key, right, level, bt);
        }
    }
}

void subtree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
                        bool *is_leftmost_node, bpnode **left_sibling, btree *bt) {
    if(!flag) return;
    if(level > this->dram_ptr->hdr.level)
        return;

    bpnode *p = this->dram_ptr;

    while(p->hdr.level > level) {
        p = (bpnode *)p->linear_search(key);
    }

    if((char *)p->hdr.leftmost_ptr == ptr) {
        *is_leftmost_node = true;
        *left_sibling = (bpnode *)p->records[0].ptr;
        return;
    }

    *is_leftmost_node = false;

    for(int i=0; p->records[i].ptr != NULL; ++i) {
        if(p->records[i].ptr == ptr) {
            if(i == 0) {
                if((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = p->hdr.leftmost_ptr;
                    p->remove(bt, *deleted_key, false, false, this);
                    break;
                }
            }
            else {
                if(p->records[i - 1].ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = (bpnode *)p->records[i - 1].ptr;
                    p->remove(bt, *deleted_key, false, false, this);
                    break;
                }
            }
        }
    }
}

void subtree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
                        bool *is_leftmost_node, nvmpage **left_sibling, btree *bt) {
    if(flag)  return;
    nvmpage* root = get_nvmroot_ptr();
    if (level > root->hdr.level)
        return;

    nvmpage* p = root;

    while (p->hdr.level > level) {
        p = to_nvmpage(p->linear_search(key));
    }

    if ((char *)p->hdr.leftmost_ptr == ptr) {
        *is_leftmost_node = true;
        *left_sibling = to_nvmpage(p->records[0].ptr);
        return;
    }

    *is_leftmost_node = false;

    for (int i = 0; p->records[i].ptr != NULL; ++i) {
        if (p->records[i].ptr == ptr) {
            if (i == 0) {
                if ((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = to_nvmpage(p->hdr.leftmost_ptr);
                    p->remove(bt, *deleted_key, false, false, this);
                    break;
                }
            } else {
                if (p->records[i - 1].ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = to_nvmpage(p->records[i - 1].ptr);
                    p->remove(bt, *deleted_key, false, false, this);
                    break;
                }     
            }
        }
    }
}


void subtree::subtree_search_range(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size) {
  if (flag) {
    bpnode* p = dram_ptr;
    while(p->hdr.leftmost_ptr != NULL) {
      p = (bpnode *)p->linear_search(min);
    }

    p->linear_search_range(min, max, results, size, (uint64_t)pop);
  } else {
    nvmpage* p = get_nvmroot_ptr();
    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(min));
    }

    p->linear_search_range(min, max, results, size, (uint64_t)pop);
  }
}

void subtree::subtree_search_range(entry_key_t min, entry_key_t max, void **values, int &size) {
  if (flag) {
    bpnode* p = dram_ptr;
    while(p->hdr.leftmost_ptr != NULL) {
      p = (bpnode *)p->linear_search(min);
    }

    p->linear_search_range(min, max, values, size, (uint64_t)pop);
  } else {
    nvmpage* p = get_nvmroot_ptr();
    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(min));
    }

    p->linear_search_range(min, max, values, size, (uint64_t)pop);
  }
}

bool subtree::rebalance(btree * bt) {
  subtree * left_subtree_sibling = rt->left;
  subtree * sub_root = rt->right;
  entry_key_t deleted_key_from_parent = rt->deleted_key_from_parent;

  if (rt->cur_n) {
    bpnode * left_dram_sibling = left_subtree_sibling->dram_ptr;
    // 不同介质间的合并操作
    rt->cur_n->merge(bt, left_dram_sibling, deleted_key_from_parent ,sub_root, left_subtree_sibling);
  } else if (rt->cur_d) {
    nvmpage* left_nvm_sibling = left_subtree_sibling->get_nvmroot_ptr();
    // 不同介质间的合并操作
    rt->cur_d->merge(bt, left_nvm_sibling, deleted_key_from_parent ,sub_root, left_subtree_sibling);
  } else {
    printf("error rebalance\n");
  }
  return true;
}

bpnode *subtree::getLastDDataNode() {
  bpnode * ret = dram_ptr;
  while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
    ret = (bpnode *)ret->records[ret->hdr.last_index].ptr;
  }
  return ret;
}

nvmpage *subtree::getLastNDataNode() {
  nvmpage * ret = get_nvmroot_ptr();
  while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
    ret = to_nvmpage(ret->records[ret->hdr.last_index].ptr);
  }
  return ret;
}

void *subtree::getLastLeafNode() {
  if (flag) {
    return (void *)getLastDDataNode();
  } else {
    return (void *)getLastNDataNode();
  }
}

bpnode *subtree::getDramDataNode(char *ptr) {
  bpnode * ret = (bpnode *)ptr;
  while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
    ret = (bpnode *)ret->records[ret->hdr.last_index].ptr;
  }
  return ret;
}

nvmpage *subtree::getNvmDataNode(char *ptr) {
  nvmpage * ret = to_nvmpage(ptr);
  while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
    ret = to_nvmpage(ret->records[ret->hdr.last_index].ptr);
  }
  return ret;
}

entry_key_t subtree::getFirstKey() {
  if (flag) {
    bpnode * ret = (bpnode *)dram_ptr;
    while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
      ret = ret->hdr.leftmost_ptr;
    }
    return ret->records[0].key;
  } else {
    nvmpage * ret = get_nvmroot_ptr();
    while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
      ret = to_nvmpage(ret->hdr.leftmost_ptr);
    }
    return ret->records[0].key;
  }
}

void subtree::recover() {
  // 遍历日志 根据type不同进行不同的操作
  if (!flag) {
    return;
  }
  if (log_off == -1) {
    printf("error: dram subtree don`t have log\n");
    return;
  }
  // 恢复出原来的日志
  LogAllocator* old_log_alloc = node_alloc->getNVMptr(log_off);
  old_log_alloc->recovery(log_alloc_pool);
  // 将原nvm子树转换成dram子树 暂时不考虑没有nvm备份的情况（可能是分裂产生的子树）
  // bpnode *pre = nullptr;
  // if (this->getPrePtr() != nullptr) {
  //   pre = (bpnode *)this->getPrePtr()->getLastLeafNode();
  // }
  // nvm_to_dram(&pre, true);

  LogNode* tmp;
  int j = 0;
  for (int i = 0; (tmp = old_log_alloc->getNextLogNode(i)) != nullptr; i++) {
    nvmpage * p = to_nvmpage((char *)tmp->off);
    j++;
    switch (tmp->type)
    {
    case 1:
      {// insert todo：存在就不insert
        int num_entries = p->count();
        p->insert_key(pop, tmp->key, (char *)tmp->value, &num_entries,true);
        break;
      }
    case 2:
      {// update
        p->update_key(pop, tmp->key, (char *)tmp->value);
        break;
      }
    case 0:
      {// delete
        p->remove_key(pop, tmp->key);
        break;
      }
    case 3:
      {
        // 子树内分裂
        nvmpage * dst = to_nvmpage((char *)tmp->key);
        int m = tmp->value;
        int sibling_cnt = 0;
        int num_entries = p->count();
        if(p->hdr.leftmost_ptr == NULL){ // leaf node
          for(int i=m; i<num_entries; ++i){ 
            dst->insert_key(pop, p->records[i].key, p->records[i].ptr, &sibling_cnt, false);
          }
        }
        else{ // internal node
          for(int i=m+1;i<num_entries;++i){ 
            dst->insert_key(pop, p->records[i].key, p->records[i].ptr, &sibling_cnt, false);
          }
          dst->hdr.leftmost_ptr = (nvmpage*) p->records[m].ptr;
        }
        dst->hdr.sibling_ptr = p->hdr.sibling_ptr;
        dst->hdr.level = p->hdr.level;
        pmemobj_persist(pop, dst, sizeof(nvmpage));

        p->hdr.sibling_ptr = (nvmpage *)pmemobj_oid(dst).off;
        pmemobj_persist(pop, &(p->hdr), sizeof(p->hdr));

        // set to NULL
        if (IS_FORWARD(p->hdr.switch_counter))
          p->hdr.switch_counter += 2;
        else
          ++p->hdr.switch_counter;
        p->records[m].ptr = NULL;
        pmemobj_persist(pop, &(p->records[m]), sizeof(nvmentry));

        p->hdr.last_index = m - 1;
        pmemobj_persist(pop, &(p->hdr.last_index), sizeof(int16_t));

        num_entries = p->hdr.last_index + 1;
        break;
      }
    case 4:
      printf("子树间分裂\n");
      // 只有在分裂后 同步前程序挂掉才会出现 此时新分裂的节点 有三种情况
      // 1.已经同步完成  无log/有log log第一条不为分裂操作 nvm子树有内容
      // 2.同步了一半   有log log第一条为分裂操作 nvm子树有内容
      // 3.还未进行同步 有log log第一条为分裂操作
      // a.当前节点未进行同步  1，2，3皆可出现
      // b.当前节点进行了同步  对应1
      break;
    case 5:
      {// 子树内合并 dram -- dram
        nvmpage * cur = to_nvmpage((char *)tmp->key);
        register int num_entries = cur->count();
        register int left_num_entries = p->count();
        entry_key_t deleted_key_from_parent = tmp->value;

        int total_num_entries = num_entries + left_num_entries;
        if (cur->hdr.leftmost_ptr != nullptr)
          ++total_num_entries;

        if (total_num_entries > nvm_cardinality - 1) { //Redistribution
          register int m = (int)ceil(total_num_entries / 2);

          if (num_entries < left_num_entries) { // left -> right
            //printf("子树内合并 dram --> dram\n");
            if (cur->hdr.leftmost_ptr == nullptr) {
              for (int i = left_num_entries - 1; i >= m; i--) {
                cur->insert_key(pop, p->records[i].key,
                            p->records[i].ptr, &num_entries);
              }

              p->records[m].ptr = nullptr;
              pmemobj_persist(pop, &(p->records[m].ptr),
                              sizeof(char *));

              p->hdr.last_index = m - 1;
              pmemobj_persist(pop, &(p->hdr.last_index), 
                              sizeof(int16_t));
            } else {
              cur->insert_key(pop, deleted_key_from_parent, (char *)cur->hdr.leftmost_ptr,
                          &num_entries);

              for (int i = left_num_entries - 1; i > m; i--) {
                cur->insert_key(pop, p->records[i].key,
                            p->records[i].ptr, &num_entries);
              }

              cur->hdr.leftmost_ptr = (nvmpage *)p->records[m].ptr;
              pmemobj_persist(pop, &(cur->hdr.leftmost_ptr), sizeof(nvmpage *));

              p->records[m].ptr = nullptr;
              pmemobj_persist(pop, &(p->records[m].ptr),
                              sizeof(char *));

              p->hdr.last_index = m - 1;
              pmemobj_persist(pop, &(p->hdr.last_index),
                              sizeof(int16_t));
            }
          } else { // left <- right
            //printf("子树内合并 dram <-- dram\n");
            int num_dist_entries = num_entries - m;
            int new_sibling_cnt = 0;
            bpnode * node_tmp = new bpnode();
            memcpy(node_tmp, cur, sizeof(bpnode));

            if (cur->hdr.leftmost_ptr == nullptr) {
              for (int i = 0; i < num_dist_entries; i++) {
                p->insert_key(pop, cur->records[i].key, cur->records[i].ptr,
                                  &left_num_entries);
              }

              for (int i = num_dist_entries; node_tmp->records[i].ptr != NULL; i++) {
                    cur->insert_key(pop, node_tmp->records[i].key, node_tmp->records[i].ptr,
                                  &new_sibling_cnt, false);
              }

              pmemobj_persist(pop, cur, sizeof(nvmpage));

              p->hdr.sibling_ptr = (nvmpage *)pmemobj_oid(cur).off;
              pmemobj_persist(pop, &(p->hdr.sibling_ptr),
                              sizeof(nvmpage *));
            } else {
              p->insert_key(pop, deleted_key_from_parent,
                                (char *)cur->hdr.leftmost_ptr, &left_num_entries);

              for (int i = 0; i < num_dist_entries - 1; i++) {
                p->insert_key(pop, cur->records[i].key, cur->records[i].ptr,
                                  &left_num_entries);
              }

              cur->hdr.leftmost_ptr =
                  (nvmpage *)node_tmp->records[num_dist_entries - 1].ptr;
              for (int i = num_dist_entries; node_tmp->records[i].ptr != NULL; i++) {
                    cur->insert_key(pop, node_tmp->records[i].key, node_tmp->records[i].ptr,
                                  &new_sibling_cnt, false);
              }
              pmemobj_persist(pop, cur, sizeof(nvmpage));
            }
          }
        } else {
          //printf("子树内合并 dram <-- dram 全部\n");
          cur->hdr.is_deleted = 1;
          pmemobj_persist(pop, &(cur->hdr.is_deleted), sizeof(uint8_t));

          if (cur->hdr.leftmost_ptr)
            p->insert_key(pop, deleted_key_from_parent,
                              (char *)cur->hdr.leftmost_ptr, &left_num_entries);

          for (int i = 0; cur->records[i].ptr != NULL; ++i) {
            p->insert_key(pop, cur->records[i].key, cur->records[i].ptr,
                              &left_num_entries);
          }

          if (cur->hdr.leftmost_ptr == nullptr) {
            p->hdr.sibling_ptr = cur->hdr.sibling_ptr;
            pmemobj_persist(pop, &(p->hdr.sibling_ptr),
                            sizeof(nvmpage *));
          }
        }
      }
      break;
    default:
      {
        printf("other type %lu\n",tmp->type);
        break;
      }
    }
  }
  printf("subtree %p recover log num: %d\n", this, j);
  this->flag = false;
  old_log_alloc->DeleteLog();
  log_off = -1;
  log_alloc = nullptr;
}

void subtree::recovery() {
  if (!flag) {
    return;
  }
  if (transing) {
    printf("error: this subtree was transing\n");
    return;
  }
  // 恢复出原来的日志
  LogAllocator* old_log_alloc = node_alloc->getNVMptr(log_off);
  old_log_alloc->recovery(log_alloc_pool);

  bpnode *pre = nullptr;
  if (this->getPrePtr() != nullptr) {
    pre = (bpnode *)this->getPrePtr()->getLastLeafNode();
  }
  flag = false;
  dram_recovery(&pre);

  SimpleLogNode* tmp;
  int j = 0;
  for (int i = 0; (tmp = old_log_alloc->getNextSimpleLogNode(i)) != nullptr; i++) {
    j++;
    switch (tmp->type)
    {
    case 1:
      {
        break;
      }
    case 2:
      {
        break;
      }
    case 0:
      {
        break;
      }
    default:
      break;
    }
  }
  printf("subtree %p recover log num: %d\n", this, j);
}

void MyBtree::constructor(PMEMobjpool * pool) {
  pop = pool;
  head = nullptr;
  time_ = 60; // 1min
  subtree_num = 20;  
  bt = new btree(pool);
  switch_ = true;

  pmemobj_persist(pop, this, sizeof(MyBtree));
}

void MyBtree::Recover(PMEMobjpool *pool) {
  pop = pool;
  if (head == nullptr) { // 没有进行转换 读log恢复
    bt = new btree(pop);
    // 进行相关操作
  } else {
    // 1.遍历subtree 恢复nvm子树
    // 2.恢复索引
    // 3.重新分布子树
    uint64_t start_time, end_time;
    start_time = get_now_micros();

    subtree *ptr = to_nvmptr(head);
    if (ptr == nullptr) {
      bt = new btree(pop);
      return;
    }
    bt = new btree(pop, 5);
    bt->setFlag2(true);
    ptr->pop = pool;
    //ptr->recover();
    ptr->recovery();
    ptr = to_nvmptr(ptr->sibling_ptr);
    while (ptr != nullptr) {
      ptr->pop = pool;
      //ptr->recover();
      ptr->recovery();
      bt->btreeInsert(ptr->getFirstKey(), (char *)ptr);
      ptr = to_nvmptr(ptr->sibling_ptr);
    }
    bt->setLeftmostPtr((bpnode *)to_nvmptr(head));
    bt->setFlag(true);
    bt->setFlag2(false);
    bt->CalcuRootLevel();
    switch_ = true;
    this->later();

    end_time = get_now_micros();
    printf("recover btree time: %f s\n", (end_time - start_time) * 1e-6);
  }
}

void MyBtree::Redistribute() {
  if (head == nullptr) {
    printf("redistribute error\n");
    return ;
  }

  // subtree node 优先队列 前subtree_num个 作为dram节点 其他的作为nvm节点
  std::priority_queue<subtree *, vector<subtree *>, cmp> q;
  subtree *ptr = to_nvmptr(head);
  int i = 0, j = 0;
  while (ptr != nullptr) {
    i++;
    if (q.size() < subtree_num) {
      q.push(ptr);
    } else if (!q.empty() && q.top()->getHeat() < ptr->getHeat()){
      q.pop();
      q.push(ptr);
    }
    ptr->lock = true;
    ptr->change = false;
    ptr = to_nvmptr(ptr->sibling_ptr);
  }

  while(!q.empty()) {
    j++;
    q.top()->change = true;
    q.top()->PrintInfo();
    q.pop();
  }
  ptr = to_nvmptr(head);

  while(ptr != nullptr) {
    ptr->lock = false;
    ptr->heat /= 2;
    ptr = to_nvmptr(ptr->sibling_ptr);
  }
  printf("\nredistribute end all: %d dram: %d \n\n", i, j);
}

void MyBtree::later() {
  std::thread([this]() {
      while(switch_) {
          Redistribute();
          std::this_thread::sleep_for(std::chrono::seconds(time_));
      }
  }).detach();
}

void MyBtree::exitBtree() {
  switch_ = false;
  subtree *ptr = to_nvmptr(head);
  while (ptr != nullptr) {
    nvmpage *pre = nullptr;
    if (ptr->getPrePtr() != nullptr) {
      pre = (nvmpage *)ptr->getPrePtr()->getLastLeafNode();
    }
    ptr->dram_to_nvm(&pre);
    ptr = to_nvmptr(ptr->sibling_ptr);
  }
  delete bt;
  pmemobj_close(pop);
}