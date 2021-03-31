#include "single_btree.h"
#include "single_pmdk.h"

Statistic stats_leaf;
/*
 *  class btree
 */

void bpnode::linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size, uint64_t base) {
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
                                    // buf[off++] = (unsigned long)tmp_ptr;
                                    // values.push_back(string(tmp_ptr, NVM_ValueSize));
                                    results.push_back({tmp_key, (uint64_t)tmp_ptr});
                                    off++;
                                    if(off >= size) {
                                        return ;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        size = off;
                        return;
                    }
                }

                for(i=1; current->records[i].ptr != NULL; ++i) { 
                    if((tmp_key = current->records[i].key) > min) {
                        if(tmp_key < max) {
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        // buf[off++] = (unsigned long)tmp_ptr;
                                        // values.push_back(string(tmp_ptr, NVM_ValueSize));
                                        results.push_back({tmp_key, (uint64_t)tmp_ptr});
                                        off++;
                                        if(off >= size) {
                                            return ;
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            size = off;
                            return;
                        }
                    }
                }
            }
            else {
                for(i=count() - 1; i > 0; --i) { 
                    if((tmp_key = current->records[i].key) > min) {
                        if(tmp_key < max) {
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        // buf[off++] = (unsigned long)tmp_ptr;
                                        // values.push_back(string(tmp_ptr, NVM_ValueSize));
                                        results.push_back({tmp_key, (uint64_t)tmp_ptr});
                                        off++;
                                        if(off >= size) {
                                            return ;
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            size = off;
                            return;
                        }
                    }
                }

                if((tmp_key = current->records[0].key) > min) {
                    if(tmp_key < max) {
                        if((tmp_ptr = current->records[0].ptr) != NULL) {
                            if(tmp_key == current->records[0].key) {
                                if(tmp_ptr) {
                                    // buf[off++] = (unsigned long)tmp_ptr;
                                    // values.push_back(string(tmp_ptr, NVM_ValueSize));
                                    results.push_back({tmp_key, (uint64_t)tmp_ptr});
                                    off++;
                                    if(off >= size) {
                                        return ;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        size = off;
                        return;
                    }
                }
            }
        } while(previous_switch_counter != current->hdr.switch_counter);

        if (IS_VALID_PTR(current->hdr.sibling_ptr) || base == 0) {
          current = current->hdr.sibling_ptr;
        } else {
          current = (bpnode *)((uint64_t)current->hdr.sibling_ptr + base);
        }
    }
    size = off;
}

void bpnode::linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size, uint64_t base) {
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
                                    // buf[off++] = (unsigned long)tmp_ptr;
                                    values[off] = tmp_ptr;
                                    off++;
                                    if(off >= size) {
                                        return ;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        size = off;
                        return;
                    }
                }

                for(i=1; current->records[i].ptr != NULL; ++i) { 
                    if((tmp_key = current->records[i].key) > min) {
                        if(tmp_key < max) {
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        // buf[off++] = (unsigned long)tmp_ptr;
                                        values[off] = tmp_ptr;
                                        off++;
                                        if(off >= size) {
                                            return ;
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            size = off;
                            return;
                        }
                    }
                }
            }
            else {
                for(i=count() - 1; i > 0; --i) { 
                    if((tmp_key = current->records[i].key) > min) {
                        if(tmp_key < max) {
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        // buf[off++] = (unsigned long)tmp_ptr;
                                        values[off] = tmp_ptr;
                                        off++;
                                        if(off >= size) {
                                            return ;
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            size = off;
                            return;
                        }
                    }
                }

                if((tmp_key = current->records[0].key) > min) {
                    if(tmp_key < max) {
                        if((tmp_ptr = current->records[0].ptr) != NULL) {
                            if(tmp_key == current->records[0].key) {
                                if(tmp_ptr) {
                                    // buf[off++] = (unsigned long)tmp_ptr;
                                    values[off] = tmp_ptr;
                                    off++;
                                    if(off >= size) {
                                        return ;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        size = off;
                        return;
                    }
                }
            }
        } while(previous_switch_counter != current->hdr.switch_counter);

        if (IS_VALID_PTR(current->hdr.sibling_ptr) || base == 0) {
          current = current->hdr.sibling_ptr;
        } else {
          current = (bpnode *)((uint64_t)current->hdr.sibling_ptr + base);
        }
    }
    size = off;
}

btree::btree(PMEMobjpool *pool){
  root = (char*)new bpnode();
  print_log(LV_DEBUG, "root is %p, btree is %p", root, this);
  height = 1;
  flag = false;
  flag2 = false;
  total_size = 0;
  pop = pool;
  log_off = getNewLogAllocator();
  log_alloc = node_alloc->getNVMptr(log_off);
  log_alloc->setCapacity(BigLogSize);
}

btree::btree(PMEMobjpool *pool, uint32_t level){
  root = (char*)new bpnode(level);
  print_log(LV_DEBUG, "root is %p, btree is %p", root, this);
  height = level + 1;
  flag = false;
  flag2 = false;
  total_size = 0;
  pop = pool;
}

btree::btree(bpnode *root_) {
    if(root_ == nullptr) {
        root = (char*)new bpnode();
        height = 1;
        flag = false;
        flag2 = false;
        total_size = 0;
    } else {
        root = (char *)root_;
        height = root_->GetLevel() + 1;
    }
    print_log(LV_DEBUG, "root is %p, btree is %p, height is %d", root, this, height);
}

void btree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  ++height;
}

void btree::setLeftmostPtr(bpnode *ptr) {
  bpnode * ret = (bpnode *)root;
  while(ret != nullptr && ret->hdr.leftmost_ptr != nullptr) {
    ret = ret->hdr.leftmost_ptr;
  }
  ret->hdr.leftmost_ptr = ptr;
}

char *btree::findSubtreeRoot(entry_key_t key) {
    if (!flag)  return nullptr;

    bpnode* p = (bpnode *)root;
    while (p->hdr.level != tar_level && p->hdr.leftmost_ptr != nullptr) {
        p = (bpnode *)p->linear_search(key);
    }

    char *ret = p->linear_search(key);
    subtree *tmp = (subtree *)ret;
    tmp->increaseHeat();
    if (tmp->change != tmp->flag && tmp->lock == false) {
      if (tmp->getState()) {
        bpnode *pre = nullptr;
        if (tmp->getPrePtr() != nullptr) {
          pre = (bpnode *)tmp->getPrePtr()->getLastLeafNode();
        }
        tmp->nvm_to_dram(&pre);
      } else {
        nvmpage *pre = nullptr;
        if (tmp->getPrePtr() != nullptr) {
          pre = (nvmpage *)tmp->getPrePtr()->getLastLeafNode();
        }
        tmp->dram_to_nvm(&pre);
      }
    }
    if(tmp->log_alloc != nullptr && tmp->log_alloc->StorageIsFull()) {
      printf("log will be full, sync!\n");
      nvmpage *pre = nullptr;
      tmp->sync_subtree(&pre);
    }
    return ret;
}

char *btree::btreeSearch(entry_key_t key) {
    if (flag) {
        subtree* sub_root = (subtree*)findSubtreeRoot(key);
        return sub_root->subtree_search(key);
    } else {
        return btree_search(key);
    }
}

char *btree::btree_search(entry_key_t key){
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode *)p->linear_search(key);
  }

  bpnode *t = (bpnode *)p->linear_search(key);
  if(!t) {
    // printf("NOT FOUND %llx, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

void btree::btreeInsert(entry_key_t key, char* right) {
    if (!flag && !flag2 && /*total_size >= MAX_DRAM_BTREE_SIZE*/ ((bpnode *)root)->hdr.level == 7) {
       CalcuRootLevel();
       deform();
    }
    if (flag) {
        subtree* sub_root = (subtree*)findSubtreeRoot(key);
        sub_root->subtree_insert(this, key, right);
    } else {
        btree_insert(key, right);
        if (log_alloc) log_alloc->writeKv(key,right);
        //total_size += sizeof(key) + sizeof(right);
    }
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char* right){ //need to be string
  bpnode* p = (bpnode*)root;

  if (flag2) {
    while(p->hdr.leftmost_ptr != NULL && p->hdr.level != 5) {
      p = (bpnode*)p->linear_search(key);
    }
  } else {
    while(p->hdr.leftmost_ptr != NULL) {
      p = (bpnode*)p->linear_search(key);
    }
  }

  if(!p->store(this, NULL, key, right)) { // store 
    btree_insert(key, right);
  }
}

void btree::btreeUpdate(entry_key_t key, char* right) {
    if (flag) {
        subtree* sub_root = (subtree*)findSubtreeRoot(key);
        sub_root->subtree_update(this, key, right);
    } else {
        btree_update(key, right);
        if (log_alloc) log_alloc->updateKv(key,right);
    }
}

// update the key in the leaf node
void btree::btree_update(entry_key_t key, char* right){ //need to be string
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode*)p->linear_search(key);
  }

  if(!p->update_key(key, right)) { // store 
    // printf("update key failed no such key\n");
  }
}

// store the key into the node at the given level 
void btree::btree_insert_internal
(char *left, entry_key_t key, char *right, uint32_t level) {
  if(level > ((bpnode *)root)->hdr.level)
    return;

  bpnode *p = (bpnode *)this->root;

  while(p->hdr.level > level) 
    p = (bpnode *)p->linear_search(key);

  if(!p->store(this, NULL, key, right)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btreeDelete(entry_key_t key) {
    if (flag) {
        subtree* sub_root = (subtree*)findSubtreeRoot(key);
        sub_root->subtree_delete(this, key);
        if (sub_root->needRebalance()) {
          sub_root->rebalance(this);
          sub_root->deleteRt();
        }
    } else {
        btree_delete(key);
        if (log_alloc) log_alloc->deleteKey(key);
    }
}

void btree::btree_delete(entry_key_t key) {
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL){
    p = (bpnode*) p->linear_search(key);
  }

  bpnode *t = (bpnode *)p->linear_search(key);
  if(p && t) {
    if(!p->remove(this, key)) {
      btree_delete(key);
    }
  }
  else {
      ;
    // printf("not found the key to delete %llx\n", key);
  }
}

void btree::btree_delete_internal
(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
bool *is_leftmost_node, bpnode **left_sibling) {
  if(level > ((bpnode *)this->root)->hdr.level)
    return;

  bpnode *p = (bpnode *)this->root;

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
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
      else {
        if(p->records[i - 1].ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = (bpnode *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }
}

// Function to search keys from "min" to "max"
void btree::btree_search_range
(entry_key_t min, entry_key_t max, unsigned long *buf) {
  bpnode *p = (bpnode *)root;

  while(p) {
    if(p->hdr.leftmost_ptr != NULL) {
      // The current bpnode is internal
      p = (bpnode *)p->linear_search(min);
    }
    else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

void btree::btreeSearchRange(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size) {
    if (flag) {
        subtree* sub_root = (subtree*)findSubtreeRoot(min);
        sub_root->subtree_search_range(min, max, results, size);
    } else {
        btree_search_range(min, max, results, size);
    }
}

void btree::btree_search_range(entry_key_t min, entry_key_t max, std::vector<std::pair<uint64_t, uint64_t>>& results, int &size) {
    bpnode *p = (bpnode *)root;

    while(p) {
        if(p->hdr.leftmost_ptr != NULL) {
        // The current bpnode is internal
            p = (bpnode *)p->linear_search(min);
        }
        else {
        // Found a leaf
            p->linear_search_range(min, max, results, size);

        break;
        }
    }
}

void btree::btreeSearchRange(entry_key_t min, entry_key_t max, void **values, int &size) {
    if (flag) {
        subtree* sub_root = (subtree*)findSubtreeRoot(min);
        sub_root->subtree_search_range(min, max, values, size);
    } else {
        btree_search_range(min, max, values, size);
    }
}

void btree::btree_search_range(entry_key_t min, entry_key_t max, void **values, int &size) {
    bpnode *p = (bpnode *)root;

    while(p) {
        if(p->hdr.leftmost_ptr != NULL) {
        // The current bpnode is internal
            p = (bpnode *)p->linear_search(min);
        }
        else {
        // Found a leaf
            p->linear_search_range(min, max, values, size);

        break;
        }
    }
}

void btree::printAll(){
  int total_keys = 0;
  bpnode *leftmost = (bpnode *)root;
  printf("root: %x\n", root);
  if(root) {
    if (flag) {

    } else {
      do {
        bpnode *sibling = leftmost;
        while(sibling) {
          if(sibling->hdr.level == 0) {
            total_keys += sibling->hdr.last_index + 1;
          }
          sibling->print();
          sibling = sibling->hdr.sibling_ptr;
        }
        printf("-----------------------------------------\n");
        leftmost = leftmost->hdr.leftmost_ptr;
      } while(leftmost);
    }
  }

  printf("total number of keys: %d\n", total_keys);
}

void btree::CalculateSapce(uint64_t &space) {
    if(root != nullptr) {
        ((bpnode*)root)->CalculateSapce(space);
    }
}

void btree::PrintInfo() {
    printf("This is a b+ tree.\n");
    printf("Node size is %lu, M path is %d.\n", sizeof(bpnode), cardinality);
    printf("Tree height is %d.\n", height);

}

void btree::CalcuRootLevel() {
    tar_level = 7;
}

void btree::deform() {
    bpnode* btree_root = (bpnode *)root;
  {  // for debug
    printf("start trans...\n");
    bpnode* tmp = btree_root->hdr.leftmost_ptr;
    tmp = tmp->hdr.leftmost_ptr;
    int tmpcnt = 1;
    int valuecnt = 0;
    while (tmp->hdr.sibling_ptr != nullptr) {
      ++tmpcnt;
      valuecnt += tmp->hdr.last_index + 1;
      tmp = tmp->hdr.sibling_ptr;
    }
    printf("****tmp level %d, node num %d, entry num %d****\n",
               tmp->hdr.level, tmpcnt, valuecnt);
  }

  // 找到root nodes层
  bpnode* p = btree_root;
  while (p->hdr.level != tar_level && p->hdr.leftmost_ptr != nullptr) {
    p = p->hdr.leftmost_ptr;
  }

  {  // for debug
    printf("bptree height:%d, roots_level:%d, calculevel:%d\n", height,
               p->hdr.level, tar_level);
    bpnode* tmp = p;
    int tmpcnt = 0;
    int valuecnt = 0;
    while (tmp != nullptr) {
      ++tmpcnt;
      valuecnt += tmp->hdr.last_index + 1;
      tmp = tmp->hdr.sibling_ptr;
    }
    printf("****subtree level %d, node num %d, entry num %d****\n",
               p->hdr.level, tmpcnt, valuecnt);
  }
    uint64_t start_time, end_time;
    start_time = get_now_micros();
    printf("subtree root start \n");
    bpnode* q = p;
    nvmpage * pre = nullptr;
    subtree * subtree_pre = nullptr;
    while(q) {
        q->hdr.leftmost_ptr = (bpnode *)newSubtreeRoot(pop, q->hdr.leftmost_ptr);
        subtree *tmp = (subtree *)q->hdr.leftmost_ptr;
        tmp->sync_subtree(&pre);
        if (subtree_pre != nullptr) {
          tmp->setPrePtr((subtree *)pmemobj_oid(subtree_pre).off);
          subtree_pre->setSiblingPtr((subtree *)pmemobj_oid(tmp).off);
        }
        subtree_pre = tmp;
        //tmp->nvm_to_dram();
        MyBtree::getInitial()->setHead((subtree *)pmemobj_oid(tmp).off);
        for (int i = 0; i <= q->hdr.last_index; i++) {
            q->records[i].ptr = (char *)newSubtreeRoot(pop, (bpnode *)q->records[i].ptr);
            subtree *tmp = (subtree *)q->records[i].ptr;
            tmp->sync_subtree(&pre);
            tmp->setPrePtr((subtree *)pmemobj_oid(subtree_pre).off);
            subtree_pre->setSiblingPtr((subtree *)pmemobj_oid(tmp).off);
            subtree_pre = tmp;
            //tmp->nvm_to_dram();
        }
        q = q->hdr.sibling_ptr;
    }
    MyBtree::getInitial()->later();
    flag = true;
    end_time = get_now_micros();
    printf("subtree root end  time: %f s\n", (end_time - start_time) * 1e-6);
}

void btree::scan_all_leaf() {
    bpnode* p = (bpnode *)root;
    while (p->hdr.level != tar_level && p->hdr.leftmost_ptr != nullptr) {
        p = (bpnode *)p->hdr.leftmost_ptr;
    }
    subtree* sub_root = (subtree *)p;
    bpnode* leaf = nullptr;
    if (sub_root->flag) {
      leaf = getFirstDDataNode();
    } else {
      leaf = (bpnode *)getFirstNDataNode();
    }
    int totul_num = 0;
    while (leaf != nullptr) {
      totul_num += leaf->hdr.last_index + 1;
      if (IS_VALID_PTR(current->hdr.sibling_ptr)) {
        leaf = (bpnode *)((uint64_t)current->hdr.sibling_ptr + pop);
      } else {
        leaf = current->hdr.sibling_ptr;
      }
    }
    cout << "total_nnum:" <<totul_num<<endl;
}

bool bpnode::remove(btree* bt, entry_key_t key, bool only_rebalance, bool with_lock, subtree* sub_root) {
  if(!only_rebalance) {
    register int num_entries_before = count();

    // This node is root
    if(this == (bpnode *)bt->root) {
      if(hdr.level > 0) {
        if(num_entries_before == 1 && !hdr.sibling_ptr) {
          bt->root = (char *)hdr.leftmost_ptr;

          hdr.status = 1;
        }
      }

      // Remove the key from this node
      bool ret = remove_key(key);
      return true;
    }

    bool should_rebalance = true;
    // check the node utilization
    if(num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) { 
      should_rebalance = false;
    }

    // Remove the key from this node
    // if (sub_root != NULL && hdr.level <= sub_root->dram_ptr->hdr.level) {
    //   sub_root->log_alloc->deleteKey(hdr.nvmpage_off, key);
    // }
    bool ret = remove_key(key);

    if(!should_rebalance) {
      return (hdr.leftmost_ptr == NULL) ? ret : true;
    }
  } 

  //Remove a key from the parent node
  entry_key_t deleted_key_from_parent = 0;
  bool is_leftmost_node = false;
  bpnode *left_sibling;
  nvmpage *left_nvm_sibling;
  subtree *left_subtree_sibling = sub_root;

  if (sub_root != NULL && hdr.level == sub_root->dram_ptr->hdr.level) { // subtree root
    bt->btree_delete_internal(key, (char *)sub_root, hdr.level + 1,
      &deleted_key_from_parent, &is_leftmost_node, &left_sibling);
    left_subtree_sibling = (subtree *)left_sibling;
    if (!left_subtree_sibling->isNVMBtree()) {
      left_sibling = left_subtree_sibling->dram_ptr;
    } else {
      left_nvm_sibling = left_subtree_sibling->get_nvmroot_ptr();
      if (is_leftmost_node) {
        // merge
        left_nvm_sibling->remove(bt, left_nvm_sibling->records[0].key, true, with_lock, left_subtree_sibling);
        return true;
      }
      // // 不同介质间的合并操作
      printf("不同介质合并\n");
      // merge(bt, left_nvm_sibling, deleted_key_from_parent ,sub_root, left_subtree_sibling);
      RebalanceTask * rt = new RebalanceTask(left_subtree_sibling, sub_root, this, nullptr, deleted_key_from_parent);
      sub_root->rt = rt;
      // persist
      return true;
    }
  } else if (sub_root != NULL && hdr.level < sub_root->dram_ptr->hdr.level) { // subtree node
    sub_root->btree_delete_internal(key, (char *)this, hdr.level + 1,
      &deleted_key_from_parent, &is_leftmost_node, &left_sibling, bt);
  } else {
    bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
      &deleted_key_from_parent, &is_leftmost_node, &left_sibling);
  }

  if(is_leftmost_node) {
    // only rebalance
    left_sibling->remove(bt, left_sibling->records[0].key, true,
        with_lock, left_subtree_sibling);
    return true;
  }

  register int num_entries = count();
  register int left_num_entries = left_sibling->count();

  // Merge or Redistribution
  int total_num_entries = num_entries + left_num_entries;
  if(hdr.leftmost_ptr)
    ++total_num_entries;

  entry_key_t parent_key;

  if(total_num_entries > cardinality - 1) { // Redistribution
    register int m = (int) ceil(total_num_entries / 2);

    if(num_entries < left_num_entries) { // left -> right
      if(hdr.leftmost_ptr == nullptr){
        for(int i=left_num_entries - 1; i>=m; i--){
          insert_key
            (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
        } 

        left_sibling->records[m].ptr = nullptr;

        left_sibling->hdr.last_index = m - 1;
        left_sibling->hdr.status = 0;

        parent_key = records[0].key; 
      }
      else{
        insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr,
            &num_entries); 

        for(int i=left_num_entries - 1; i>m; i--){
          insert_key
            (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
        }

        parent_key = left_sibling->records[m].key; 

        hdr.leftmost_ptr = (bpnode*)left_sibling->records[m].ptr; 

        left_sibling->records[m].ptr = nullptr;

        left_sibling->hdr.last_index = m - 1;
        left_sibling->hdr.status = 0;
      }

      if (sub_root != NULL && hdr.level == sub_root->dram_ptr->hdr.level) { // subtree root
        bt->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
        // log
        //sub_root->log_alloc->operateTree(left_sibling->hdr.nvmpage_off, hdr.nvmpage_off, parent_key, 6);
        //left_subtree_sibling->log_alloc->operateTree(left_sibling->hdr.nvmpage_off, hdr.nvmpage_off, parent_key, 6);
        // sync right left
        // nvmpage *pre = nullptr;
        // sub_root->sync_subtree(&pre);
        // pre = nullptr;
        // left_subtree_sibling->sync_subtree(&pre);

        // heat
        uint64_t l = left_subtree_sibling->getHeat();
        uint64_t r = sub_root->getHeat();
        sub_root->setHeat(r + (left_num_entries - m) / left_num_entries * l);
        left_subtree_sibling->setHeat(m / left_num_entries * l);
      }
      else if (sub_root != NULL && hdr.level < sub_root->dram_ptr->hdr.level) { // subtree node
        //sub_root->log_alloc->operateTree(left_sibling->hdr.nvmpage_off, hdr.nvmpage_off, deleted_key_from_parent, 5);
        sub_root->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)this, hdr.level + 1, bt);
      } else if(left_sibling == ((bpnode *)bt->root)) {
        bpnode* new_root = new bpnode(left_sibling, parent_key, this, hdr.level + 1);
        bt->setNewRoot((char *)new_root);
      }
      else {
        bt->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)this, hdr.level + 1);
      }
    }
    else{ // from leftmost case
      hdr.status = 1;

      bpnode* new_sibling = new bpnode(hdr.level); 
      new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      new_sibling->hdr.nvmpage_off = hdr.nvmpage_off;

      int num_dist_entries = num_entries - m;
      int new_sibling_cnt = 0;

      if(hdr.leftmost_ptr == nullptr){
        for(int i=0; i<num_dist_entries; i++){
          left_sibling->insert_key(records[i].key, records[i].ptr,
              &left_num_entries); 
        } 

        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
          new_sibling->insert_key(records[i].key, records[i].ptr,
              &new_sibling_cnt); 
        } 

        left_sibling->hdr.sibling_ptr = new_sibling;

        parent_key = new_sibling->records[0].key; 
      }
      else{
        left_sibling->insert_key(deleted_key_from_parent,
            (char*)hdr.leftmost_ptr, &left_num_entries);

        for(int i=0; i<num_dist_entries - 1; i++){
          left_sibling->insert_key(records[i].key, records[i].ptr,
              &left_num_entries); 
        } 

        parent_key = records[num_dist_entries - 1].key;

        new_sibling->hdr.leftmost_ptr = (bpnode*)records[num_dist_entries - 1].ptr;
        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
          new_sibling->insert_key(records[i].key, records[i].ptr,
              &new_sibling_cnt); 
        } 

        left_sibling->hdr.sibling_ptr = new_sibling;
      }

      // update new dram ptr
      if (sub_root != NULL && hdr.level == sub_root->dram_ptr->hdr.level) { // subtree root
        sub_root->setNewDramRoot(new_sibling);

        // log
        //sub_root->log_alloc->operateTree(hdr.nvmpage_off, left_sibling->hdr.nvmpage_off, parent_key, 7);
        //left_subtree_sibling->log_alloc->operateTree(hdr.nvmpage_off, left_sibling->hdr.nvmpage_off, parent_key, 7);
        // sync left right
        nvmpage *pre = nullptr;
        left_subtree_sibling->sync_subtree(&pre);
        pre = nullptr;
        sub_root->sync_subtree(&pre);

        // heat
        uint64_t l = left_subtree_sibling->getHeat();
        uint64_t r = sub_root->getHeat();
        sub_root->setHeat(m / num_entries * r);
        left_subtree_sibling->setHeat(l + num_dist_entries / left_num_entries * r);

        bt->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
      }
      else if (sub_root != NULL && hdr.level < sub_root->dram_ptr->hdr.level) { // subtree node
        //sub_root->log_alloc->operateTree(left_sibling->hdr.nvmpage_off, hdr.nvmpage_off, deleted_key_from_parent, 5);
        sub_root->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)new_sibling, hdr.level + 1, bt);
      } else if (left_sibling == ((bpnode *)bt->root)) {
        bpnode* new_root = new bpnode(left_sibling, parent_key, new_sibling, hdr.level + 1);
        bt->setNewRoot((char *)new_root);
      }
      else {
        bt->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)new_sibling, hdr.level + 1);
      }
    }
  }
  else {
    hdr.status = 1;
    if(hdr.leftmost_ptr)
      left_sibling->insert_key(deleted_key_from_parent, 
          (char *)hdr.leftmost_ptr, &left_num_entries);

    for(int i = 0; records[i].ptr != NULL; ++i) { 
      left_sibling->insert_key(records[i].key, records[i].ptr, &left_num_entries);
    }

    left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

    // if (sub_root != NULL && hdr.level < sub_root->dram_ptr->hdr.level) {
    //   sub_root->log_alloc->operateTree(left_sibling->hdr.nvmpage_off, hdr.nvmpage_off, deleted_key_from_parent, 5);
    // }

    // subtree root
    if (sub_root != NULL && hdr.level == sub_root->dram_ptr->hdr.level) {
      // sync
      nvmpage *pre = nullptr;
      left_subtree_sibling->sync_subtree(&pre);
      //delete sub_root
      left_subtree_sibling->setSiblingPtr(sub_root->sibling_ptr);
      left_subtree_sibling->tmp_ptr = (subtree *)pmemobj_oid(sub_root).off;
      if (left_subtree_sibling->getSiblingPtr()) 
        left_subtree_sibling->getSiblingPtr()->setPrePtr((subtree *)pmemobj_oid(left_subtree_sibling).off);

      uint64_t l = left_subtree_sibling->getHeat();
      uint64_t r = sub_root->getHeat();
      left_subtree_sibling->setHeat(l + r);

      // log
      //sub_root->log_alloc->operateTree(hdr.nvmpage_off, left_sibling->hdr.nvmpage_off, parent_key, 8);
    }
  }

  return true;
}

bool bpnode::merge(btree *bt, nvmpage *left_sibling, entry_key_t deleted_key_from_parent, subtree* sub_root, subtree* left_subtree_sibling)
{
  register int num_entries = count();
  register int left_num_entries = left_sibling->count();
  uint64_t l = left_subtree_sibling->getHeat();
  uint64_t r = sub_root->getHeat();

  // Merge or Redistribution
  int total_num_entries = num_entries + left_num_entries;
  if(hdr.leftmost_ptr)
    ++total_num_entries;
  
  if (hdr.leftmost_ptr == nullptr) {
    printf("error\n");
    return false;
  }

  entry_key_t parent_key;

  if(total_num_entries > cardinality - 1) { // Redistribution
    register int m = (int) ceil(total_num_entries / 2);

    if(num_entries < left_num_entries) { // left -> right
      {
        if (left_sibling->records[m].ptr == nullptr) {
          bt->btree_insert_internal
            ((char *)left_sibling, deleted_key_from_parent, (char *)sub_root, hdr.level + 1);
          return true;
        }
        //sub_root->log_alloc->writeKv(hdr.nvmpage_off, deleted_key_from_parent, (char*)hdr.leftmost_ptr);
        insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr,
            &num_entries); 

        //sub_root->log_alloc->writeKv(hdr.nvmpage_off, 0, left_sibling->records[m].ptr);
        bpnode * pre = (bpnode *)left_subtree_sibling->getNvmDataNode(left_sibling->records[m-1].ptr);// todo
        hdr.leftmost_ptr = (bpnode*)sub_root->DFS((nvmpage *)left_sibling->records[m].ptr, &pre); 
        for(int i=m + 1; i < left_num_entries; i++){
          //sub_root->log_alloc->writeKv(hdr.nvmpage_off, left_sibling->records[i].key, left_sibling->records[i].ptr);
          insert_key
            (left_sibling->records[i].key, sub_root->DFS((nvmpage *)left_sibling->records[i].ptr, &pre), &num_entries); 
        }

        parent_key = left_sibling->records[m].key; 

        left_sibling->records[m].ptr = nullptr;
        pmemobj_persist(bt->pop, &(left_sibling->records[m].ptr),
                        sizeof(char *));

        left_sibling->hdr.last_index = m - 1;
        pmemobj_persist(bt->pop, &(left_sibling->hdr.last_index),
                        sizeof(int16_t));
      }

      //heat
      sub_root->setHeat(r + (left_num_entries - m) / left_num_entries * l);
      left_subtree_sibling->setHeat(m / left_num_entries * l);

      bt->btree_insert_internal
          ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
    }
    else{ // from leftmost case
      hdr.status = 1;

      bpnode* new_sibling = new bpnode(hdr.level); 
      new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      new_sibling->hdr.nvmpage_off = hdr.nvmpage_off;

      int num_dist_entries = num_entries - m;
      int new_sibling_cnt = 0;

      {
        nvmpage * pre = left_subtree_sibling->getLastNDataNode();//todo
        left_sibling->insert_key(bt->pop, deleted_key_from_parent,
            sub_root->DFS((char*)hdr.leftmost_ptr, &pre), &left_num_entries);

        for(int i=0; i<num_dist_entries - 1; i++){
          left_sibling->insert_key(bt->pop, records[i].key, sub_root->DFS(records[i].ptr, &pre),
              &left_num_entries); 
        } 

        parent_key = records[num_dist_entries - 1].key;

        new_sibling->hdr.leftmost_ptr = (bpnode*)records[num_dist_entries - 1].ptr;
        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
          new_sibling->insert_key(records[i].key, records[i].ptr,
              &new_sibling_cnt); 
        } 
      }

      // update new dram ptr
      sub_root->setNewDramRoot(new_sibling);

      sub_root->setHeat(m / num_entries * r);
      left_subtree_sibling->setHeat(l + num_dist_entries / left_num_entries * r);

      // log标记合并操作
      //sub_root->log_alloc->operateTree(hdr.nvmpage_off, -1, num_dist_entries - 1, 10);

      bt->btree_insert_internal
        ((char *)left_sibling, parent_key, (char *)sub_root, hdr.level + 1);
      
    }
  }
  else {
    hdr.status = 1;

    nvmpage * pre = left_subtree_sibling->getLastNDataNode();//todo
    left_sibling->insert_key(bt->pop, deleted_key_from_parent, 
          sub_root->DFS((char*)hdr.leftmost_ptr, &pre), &left_num_entries);

    for(int i = 0; records[i].ptr != NULL; ++i) { 
      left_sibling->insert_key(bt->pop, records[i].key, sub_root->DFS(records[i].ptr, &pre), &left_num_entries);
    }

    // subtree root
    //delete sub_root
    left_subtree_sibling->setSiblingPtr(sub_root->sibling_ptr);
    if (left_subtree_sibling->getSiblingPtr()) 
      left_subtree_sibling->getSiblingPtr()->setPrePtr((subtree *)pmemobj_oid(left_subtree_sibling).off);
    left_subtree_sibling->setHeat(l + r);
  }

  return true;
}

inline bool bpnode::update_key(entry_key_t key, char* ptr) {
  register int num_entries = count();

  if (num_entries == 0) {
    return false;
  }
  int left = 0;
  int right = num_entries - 1;
  while(left <= right) { // 注意
      int mid = (right + left) / 2;
      if(records[mid].key == key) {
        if ((mid == 0 && records[mid].ptr != nullptr) || (mid > 0 && records[mid-1].ptr != records[mid].ptr)) {
          records[mid].ptr = ptr;
          hdr.status = 0;
          return true; 
        } 
        return false;
      } else if (records[mid].key < key) {
          left = mid + 1; // 注意
      } else if (records[mid].key > key) {
          right = mid - 1; // 注意
      }
  }
  return false;
}

inline void bpnode::insert_key(entry_key_t key, char* ptr, int *num_entries) {
  // update switch_counter
  if(!IS_FORWARD(hdr.switch_counter))
    ++hdr.switch_counter;

  // FAST
  if(*num_entries == 0) {  // this bpnode is empty
    entry* new_entry = (entry*) &records[0];
    entry* array_end = (entry*) &records[1];
    new_entry->key = (entry_key_t) key;
    new_entry->ptr = (char*) ptr;

    array_end->ptr = (char*)NULL;
  }
  else {
    int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
    records[*num_entries+1].ptr = records[*num_entries].ptr; 
    // clflush((char*)&(records[*num_entries+1].ptr), sizeof(char*));

    // FAST
    for(i = *num_entries - 1; i >= 0; i--) {
      if(key < records[i].key ) {
        records[i+1].ptr = records[i].ptr;
        records[i+1].key = records[i].key;
      }
      else{
        records[i+1].ptr = records[i].ptr;//保证ptr不一样的时候 是插入完成
        records[i+1].key = key;
        records[i+1].ptr = ptr;
        // clflush((char *)(&records[i+1]), sizeof(entry));
        inserted = 1;
        break;
      }
    }
    if(inserted==0){
      records[0].ptr =(char*) hdr.leftmost_ptr;
      records[0].key = key;
      records[0].ptr = ptr;
    }
  }

  hdr.last_index = *num_entries;
  ++(*num_entries);
  hdr.status = 0;
}

bpnode *bpnode::store(btree* bt, char* left, entry_key_t key, char* right,
       subtree* sub_root, bpnode *invalid_sibling) {
  // If this node has a sibling node,
  // if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
  //   // Compare this key with the first key of the sibling
  //   if(key > hdr.sibling_ptr->records[0].key) {
  //     return hdr.sibling_ptr->store(bt, NULL, key, right, 
  //         sub_root, invalid_sibling);
  //   }
  // }
  //stats_leaf.start();

  register int num_entries = count();
  // ptr
  // char *target = right;
  // if (!(hdr.leftmost_ptr == NULL && !bt->flag2)) {
  //   target = (char *)((bpnode *)right)->hdr.nvmpage_off;
  // }

  // FAST
  if(num_entries < cardinality - 1) {
    // if (sub_root != NULL && hdr.level <= sub_root->dram_ptr->hdr.level) {
    //   sub_root->log_alloc->writeKv(hdr.nvmpage_off, key, target);
    // }
    insert_key(key, right, &num_entries);
    // if (this->hdr.leftmost_ptr == nullptr) { 
    //     stats_leaf.end();
    //     stats_leaf.add_put();
    //   }
    return this;
  }
  else {// FAIR
    // overflow
    // create a new node
    bpnode* sibling = new bpnode(hdr.level); 
    register int m = (int) ceil(num_entries/2);
    entry_key_t split_key = records[m].key;

    // migrate half of keys into the sibling
    int sibling_cnt = 0;
    if(hdr.leftmost_ptr == NULL && !bt->flag2){ // leaf node
      for(int i=m; i<num_entries; ++i){ 
        sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt);
      }
    }
    else{ // internal node
      for(int i=m+1;i<num_entries;++i){ 
        sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt);
      }
      sibling->hdr.leftmost_ptr = (bpnode*) records[m].ptr;
    }

    sibling->hdr.sibling_ptr = hdr.sibling_ptr;

    hdr.sibling_ptr = sibling;

    // set to NULL
    if(IS_FORWARD(hdr.switch_counter))
      hdr.switch_counter += 2;
    else
      ++hdr.switch_counter;
    records[m].ptr = NULL;

    hdr.last_index = m - 1;
    hdr.status = 0;

    num_entries = hdr.last_index + 1;

    bpnode *ret;

    // insert the key
    if(key < split_key) {
      insert_key(key, right, &num_entries);
      ret = this;
    }
    else {
      sibling->insert_key(key, right, &sibling_cnt);
      ret = sibling;
    }
    // if (this->hdr.leftmost_ptr == nullptr) {
    //     stats_leaf.end();
    //     stats_leaf.add_put();
    //   }

    // create a new nvmpage
    if (sub_root != NULL && hdr.level <= sub_root->dram_ptr->hdr.level) {
      TOID(nvmpage) nvm_node;
      POBJ_NEW(bt->pop, &nvm_node, nvmpage, NULL, NULL);
      D_RW(nvm_node)->constructor();
      sibling->hdr.nvmpage_off = nvm_node.oid.off;
    }

    // Set a new root or insert the split key to the parent
    
    if (sub_root != NULL && hdr.level == sub_root->dram_ptr->hdr.level) { // subtree root
      subtree* next = newSubtreeRoot(bt->pop, sibling, sub_root);
      // sync
      // nvmpage *pre = nullptr;
      // next->sync_subtree(&pre);
      // pre = nullptr;
      // sub_root->sync_subtree(&pre);

      if (sub_root->getSiblingPtr()) 
        sub_root->getSiblingPtr()->setPrePtr((subtree *)pmemobj_oid(next).off);
      sub_root->setSiblingPtr((subtree *)pmemobj_oid(next).off);
      sub_root->setHeat(sub_root->heat * 2 / 3);

      // log
      // sub_root->log_alloc->operateTree(hdr.nvmpage_off, sibling->hdr.nvmpage_off, m, 4);
      // if (key < split_key) {
      //   sub_root->log_alloc->writeKv(hdr.nvmpage_off, key, target);
      // } else {
      //   next->log_alloc->writeKv(sibling->hdr.nvmpage_off, key, target);
      // }

      bt->btree_insert_internal(NULL, split_key, (char *)next, 
          hdr.level + 1);
    }
    else if (sub_root != NULL && hdr.level < sub_root->dram_ptr->hdr.level) { // subtree node
      // log
      // sub_root->log_alloc->operateTree(hdr.nvmpage_off, sibling->hdr.nvmpage_off, m, 3);
      // if (key < split_key) {
      //   sub_root->log_alloc->writeKv(hdr.nvmpage_off, key, target);
      // } else {
      //   sub_root->log_alloc->writeKv(sibling->hdr.nvmpage_off, key, target);
      // }
      sub_root->btree_insert_internal(NULL, split_key, (char *)sibling, 
          hdr.level + 1, bt);
    } else if (bt->root == (char *)this) { // only one node can update the root ptr
      bpnode* new_root = new bpnode((bpnode*)this, split_key, sibling, 
          hdr.level + 1);
      bt->setNewRoot((char *)new_root);
    }
    else { // internal node
      bt->btree_insert_internal(NULL, split_key, (char *)sibling, 
          hdr.level + 1);
    }

    return ret;
  }
}


void btree::to_nvm() {
  nvm_root = (nvmpage *)DFS(root);
}

char* btree::DFS(char* root) {
    if(root == nullptr) {
        return nullptr;
    }
    TOID(nvmpage) nvm_node;
    POBJ_NEW(pop, &nvm_node, nvmpage, NULL, NULL);
    D_RW(nvm_node)->constructor();
    nvmpage* nvm_node_ptr = D_RW(nvm_node);
    bpnode* node = (bpnode *)root;
    
    int count = 0;
    //nvm_node_ptr->hdr.status = node->hdr.status;
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
    //nvm_node_ptr->records[count].ptr = nullptr;
    pmemobj_persist(pop, nvm_node_ptr, sizeof(nvmpage));
    delete node;
    return (char *)nvm_node_ptr;
}