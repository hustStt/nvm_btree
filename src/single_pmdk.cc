#include "single_pmdk.h"
#include "single_btree.h"

/*
 * class nvmbtree
 */

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
  //left_sibling.oid.pool_uuid_lo = bt->root.oid.pool_uuid_lo;
  subtree * left_subtree_sibling;
  nvmpage * nvm_root = sub_root->get_nvmroot_ptr();

  if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
      bt->btree_delete_internal(key, (char *)sub_root, hdr.level + 1,
        &deleted_key_from_parent, &is_leftmost_node, (bpnode **)&left_sibling);
      left_subtree_sibling = (subtree *)left_sibling;
      left_sibling = left_subtree_sibling->get_nvmroot_ptr();
  } else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
      sub_root->btree_delete_internal(key, (char *)pmemobj_oid(this).off, hdr.level + 1,
        &deleted_key_from_parent, &is_leftmost_node, &left_sibling, bt);
  } else {
      printf("remove error\n");
  }

  if (is_leftmost_node) {
    left_sibling->remove(bt, left_sibling->records[0].key, true, with_lock, sub_root);
    return true;
  }

  register int num_entries = count();
  register int left_num_entries = left_sibling->count();

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
      }
      else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
          sub_root->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)pmemobj_oid(this).off, hdr.level + 1, bt);
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

        // left_sibling->hdr.sibling_ptr = new_sibling;
        // pmemobj_persist(bt->pop, &(left_sibling->hdr.sibling_ptr),
        //                 sizeof(nvmpage *));
      }

      if (sub_root != NULL && hdr.level == nvm_root->hdr.level) { // subtree root
          sub_root->nvm_ptr = (nvmpage *)new_sibling.oid.off;
          pmemobj_persist(bt->pop, sub_root, sizeof(subtree));

          bt->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
      }
      else if (sub_root != NULL && hdr.level < nvm_root->hdr.level) { // subtree node
          sub_root->btree_insert_internal
            ((char *)left_sibling, parent_key, (char *)new_sibling.oid.off, hdr.level + 1, bt);
      } else {
        printf("leftmost case error\n");
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
      left_subtree_sibling->sibling_ptr = sub_root->sibling_ptr;
      pmemobj_persist(bt->pop, left_subtree_sibling, sizeof(subtree));
    }
  }

  return true;
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

void nvmpage::linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size) {
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
    current = current->hdr.sibling_ptr;
  }
}


void subtree::subtree_insert(btree* root, entry_key_t key, char* right) {
  if (flag) {
    // write log
    log_alloc->writeKv(key, right);
    bpnode *p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL) {
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

void subtree::subtree_delete(btree* root, entry_key_t key) {
  if (flag) {
    // write log
    log_alloc->deleteKey(key);
    bpnode* p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL){
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

void subtree::nvm_to_dram() {
  if (flag) {
    return ;
  }
  flag = true;
  dram_ptr = (bpnode *)DFS(nvm_ptr, nullptr);
  log_alloc = getNewLogAllocator();
}

char* subtree::DFS(nvmpage* root, bpnode *pre) {
    if(root == nullptr) {
        return nullptr;
    }
    
    nvmpage* nvm_node_ptr = to_nvmpage(root);
    bpnode* node = new bpnode();
    
    int count = 0;
    
    node->hdr.is_deleted = nvm_node_ptr->hdr.is_deleted;
    node->hdr.last_index = nvm_node_ptr->hdr.last_index;
    node->hdr.level = nvm_node_ptr->hdr.level;
    node->hdr.switch_counter = nvm_node_ptr->hdr.switch_counter;
    node->hdr.nvmpage_off = (uint64_t)root;
    
    node->hdr.leftmost_ptr = (bpnode *)DFS(nvm_node_ptr->hdr.leftmost_ptr);
    while(nvm_node_ptr->records[count].ptr != NULL) {
        node->records[count].key = nvm_node_ptr->records[count].key;
        if (nvm_node_ptr->hdr.leftmost_ptr != nullptr) {
            node->records[count].ptr = DFS((nvmpage *)nvm_node_ptr->records[count].ptr);
        } else {
            node->records[count].ptr = nvm_node_ptr->records[count].ptr;
        }
        ++count;
    }
    node->records[count].ptr = nullptr;

    if (node->hdr.leftmost_ptr == nullptr) {
      if (pre != nullptr) {
        pre->hdr.sibling_ptr = node;
      }
      pre = node;
    }

    return (char *)node;
}

void subtree::dram_to_nvm() {
  if (!flag) {
    return ;
  }

  nvm_ptr = (nvmpage *)DFS((char *)dram_ptr, nullptr);
  flag = false;
  // delete log
  delete log_alloc;
  log_alloc = nullptr;
}

char* subtree::DFS(char* root, nvmpage *pre) {
    if(root == nullptr) {
        return nullptr;
    }
    bpnode* node = (bpnode *)root;
    nvmpage* nvm_node_ptr;
    TOID(nvmpage) nvm_node;
    char * ret;
    if (node->hdr.nvmpage_off != -1) { // 复用nvmpage
      nvm_node_ptr = to_nvmpage((char *)node->hdr.nvmpage_off);
      ret = (char *)node->hdr.nvmpage_off;
    } else { // 新节点 重新申请nvmpage
      POBJ_NEW(pop, &nvm_node, nvmpage, NULL, NULL);
      D_RW(nvm_node)->constructor();
      nvm_node_ptr = D_RW(nvm_node);
      ret = (char *)nvm_node.oid.off;
    }
    
    int count = 0;
    nvm_node_ptr->hdr.is_deleted = node->hdr.is_deleted;
    nvm_node_ptr->hdr.last_index = node->hdr.last_index;
    nvm_node_ptr->hdr.level = node->hdr.level;
    nvm_node_ptr->hdr.switch_counter = node->hdr.switch_counter;
    //sibling 
    
    nvm_node_ptr->hdr.leftmost_ptr = (nvmpage *)DFS((char *)node->hdr.leftmost_ptr);
    while(node->records[count].ptr != NULL) {
        nvm_node_ptr->records[count].key = node->records[count].key;
        if (node->hdr.leftmost_ptr != nullptr) {
            nvm_node_ptr->records[count].ptr = DFS(node->records[count].ptr);
        } else {
            nvm_node_ptr->records[count].ptr = node->records[count].ptr;
        }
        ++count;
    }
    nvm_node_ptr->records[count].ptr = nullptr;
    pmemobj_persist(pop, nvm_node_ptr, sizeof(nvmpage));

    if (node->hdr.leftmost_ptr == nullptr) {
      if (pre != nullptr) {
        pre->hdr.sibling_ptr = (nvmpage *)ret;
      }
      pre = nvm_node_ptr;
    }

    delete node;
    return ret;
}
/*
char* subtree::DFS(char* root) {
    if(root == nullptr) {
        return nullptr;
    }
    bpnode* nvm_node_ptr = new bpnode();
    bpnode* node = (bpnode *)root;
    
    int count = 0;
    nvm_node_ptr->hdr.is_deleted = node->hdr.is_deleted;
    nvm_node_ptr->hdr.last_index = node->hdr.last_index;
    nvm_node_ptr->hdr.level = node->hdr.level;
    nvm_node_ptr->hdr.switch_counter = node->hdr.switch_counter;
    //sibling 
    
    nvm_node_ptr->hdr.leftmost_ptr = (bpnode *)DFS((char *)node->hdr.leftmost_ptr);
    while(node->records[count].ptr != NULL) {
        nvm_node_ptr->records[count].key = node->records[count].key;
        if (node->hdr.leftmost_ptr != nullptr) {
            nvm_node_ptr->records[count].ptr = DFS(node->records[count].ptr);
        } else {
            nvm_node_ptr->records[count].ptr = node->records[count].ptr;
        }
        ++count;
    }
    nvm_node_ptr->records[count].ptr = nullptr;

    bpnode *tmp1;
    bpnode *tmp2;
    bpnode *tmp3;
    bpnode *tmp4;
    if (node->hdr.leftmost_ptr != nullptr) {
        tmp1 = (bpnode *)nvm_node_ptr->hdr.leftmost_ptr;
        tmp2 = (bpnode *)nvm_node_ptr->records[0].ptr;
        tmp1->hdr.sibling_ptr = tmp2;
        if (tmp1->hdr.leftmost_ptr != nullptr) {
            tmp3 = (bpnode *)tmp1->records[tmp1->hdr.last_index].ptr;
            tmp4 = (bpnode *)tmp2->hdr.leftmost_ptr;
            tmp3->hdr.sibling_ptr = tmp4;
        }
        for (int i = 0; i < node->hdr.last_index;++i) {
            tmp1 = (bpnode *)nvm_node_ptr->records[i].ptr;
            tmp2 = (bpnode *)nvm_node_ptr->records[i+1].ptr;
            tmp1->hdr.sibling_ptr = tmp2;
            if (tmp->hdr.leftmost_ptr != nullptr) {
                tmp3 = (bpnode *)tmp1->records[tmp1->hdr.last_index].ptr;
                tmp4 = (bpnode *)tmp2->hdr.leftmost_ptr;
                tmp3->hdr.sibling_ptr = tmp4;
            }
        }
    }
    delete node;
    return (char *)nvm_node_ptr;
}
*/
void subtree::sync_subtree() {
  if (flag == false) {
    return ;
  }
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


void subtree::subtree_search_range(entry_key_t min, entry_key_t max, void **values, int &size) {
  if (flag) {
    bpnode* p = dram_ptr;
    while(p->hdr.leftmost_ptr != NULL) {
      p = (bpnode *)p->linear_search(min);
    }

    p->linear_search_range(min, max, values, size);
  } else {
    nvmpage* p = get_nvmroot_ptr();
    while (p->hdr.leftmost_ptr != NULL) {
      p = to_nvmpage(p->linear_search(min));
    }

    p->linear_search_range(min, max, values, size);
  }
}