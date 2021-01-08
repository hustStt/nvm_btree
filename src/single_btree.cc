#include "single_btree.h"

/*
 *  class btree
 */

void bpnode::linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::string> &values, int &size) {
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
                                    values.push_back(string(tmp_ptr, NVM_ValueSize));
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
                                        values.push_back(string(tmp_ptr, NVM_ValueSize));
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
                                        values.push_back(string(tmp_ptr, NVM_ValueSize));
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
                                    values.push_back(string(tmp_ptr, NVM_ValueSize));
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

        current = current->hdr.sibling_ptr;
    }
    size = off;
}

void bpnode::linear_search_range(entry_key_t min, entry_key_t max, void **values, int &size) {
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

        current = current->hdr.sibling_ptr;
    }
    size = off;
}

btree::btree(PMEMobjpool *pool){
  root = (char*)new bpnode();
  print_log(LV_DEBUG, "root is %p, btree is %p", root, this);
  height = 1;
  flag = false;
  total_size = 0;
  pop = pool;
}

btree::btree(bpnode *root_) {
    if(root_ == nullptr) {
        root = (char*)new bpnode();
        height = 1;
        flag = false;
        total_size = 0;
    } else {
        root = (char *)root_;
        height = root_->GetLevel() + 1;
    }
    print_log(LV_DEBUG, "root is %p, btree is %p, height is %d", root, this, height);
}

void btree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  clflush((char*)&(this->root),sizeof(char*));
  ++height;
}

char *btree::findSubtreeRoot(entry_key_t key) {
    if (!flag)  return nullptr;

    bpnode* p = (bpnode *)root;
    while (p->hdr.level != tar_level && p->hdr.leftmost_ptr != nullptr) {
        p = p->hdr.leftmost_ptr;
    }

    char *ret = p->linear_search(key);
    return ret;
}

char *btree::btreeSearch(entry_key_t key) {
    if (flag) {
        TOID(subtree) sub_root;
        sub_root.oid.off = (uint64_t)findSubtreeRoot(key);
        return D_RW(sub_root)->subtree_search(key);
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
    if (total_size >= MAX_DRAM_BTREE_SIZE) {
        CalcuRootLevel();
        deform();
    }
    if (flag) {
        TOID(subtree) sub_root;
        sub_root.oid.off = (uint64_t)findSubtreeRoot(key);
        D_RW(sub_root)->subtree_insert(key, right);
    } else {
        btree_insert(key, right);
        total_size += sizeof(key) + sizeof(right);
    }
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char* right){ //need to be string
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode*)p->linear_search(key);
  }

  if(!p->store(this, NULL, key, right, true)) { // store 
    btree_insert(key, right);
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

  if(!p->store(this, NULL, key, right, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btreeDelete(entry_key_t key) {
    if (flag) {
        TOID(subtree) sub_root;
        sub_root.oid.off = (uint64_t)findSubtreeRoot(key);
        D_RW(sub_root)->subtree_delete(key);
    } else {
        btree_delete(key);
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

void btree::btree_search_range(entry_key_t min, entry_key_t max, std::vector<std::string> &values, int &size) {
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
    tar_level = 6;
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
    int tmpcnt = 1;
    int valuecnt = 0;
    while (tmp->hdr.sibling_ptr != nullptr) {
      ++tmpcnt;
      valuecnt += tmp->hdr.last_index + 1;
      tmp = tmp->hdr.sibling_ptr;
    }
    printf("****subtree level %d, node num %d, entry num %d****\n",
               tmp->hdr_.level_, tmpcnt, valuecnt);
  }

    bpnode* q = p;
    while(q) {
        q->hdr.leftmost_ptr = (bpnode *)pmemobj_oid(newSubtreeRoot((bpnode *)q->hdr.leftmost_ptr)).off;
        for (int i = 0; i <= q->hdr.last_index; i++) {
            q->records[i].ptr = (bpnode *)pmemobj_oid(newSubtreeRoot((bpnode *)q->records[i].ptr)).off
        }
        q = q->hrd.sibling_ptr;
    }
    flag = true;
}

void subtree::subtree_insert(entry_key_t key, char* right) {
  if (flag) {
    // write log
    bpnode *p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL) {
      p = (bpnode*)p->linear_search(key);
    }

    if(!p->store(this, NULL, key, right, true)) { // store 
      subtree_insert(key, right);
    }
  } else {
    TOID(nvmpage) p = nvm_ptr;

    while (D_RO(p)->hdr.leftmost_ptr != NULL) {
      p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
    }

    if (!D_RW(p)->store(this, NULL, key, right, true)) { // store
      subtree_insert(key, right);
    }
  }
  // 检查分裂
}

void subtree::subtree_delete(entry_key_t key) {
  if (flag) {
    bpnode* p = dram_ptr;

    while(p->hdr.leftmost_ptr != NULL){
      p = (bpnode*) p->linear_search(key);
    }

    bpnode *t = (bpnode *)p->linear_search(key);
    if(p && t) {
      if(!p->remove(this, key)) {
        subtree_delete(key);
      }
    }
    else {
        ;
      // printf("not found the key to delete %llx\n", key);
    }
  } else {
    TOID(nvmpage) p = nvm_ptr;

    while (D_RO(p)->hdr.leftmost_ptr != NULL) {
      p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
    }

    uint64_t t = (uint64_t)D_RW(p)->linear_search(key);
    if (t) {
      if (!D_RW(p)->remove(this, key)) {
        subtree_delete(key);
      }
    } else {
      // printf("not found the key to delete %lu\n", key);
    }
  }
  // 检查合并
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
    TOID(nvmpage) p = nvm_ptr;
    while (D_RO(p)->hdr.leftmost_ptr != NULL) {
      p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
    }

    uint64_t t = (uint64_t)D_RW(p)->linear_search(key);
    if (!t) {
      printf("NOT FOUND %lu, t = %x\n", key, t);
      return NULL;
    }

    return (char *)t;
  }
}

void subtree::nvm_to_dram() {
  if (flag == true) {
    return ;
  }
  flag = true;
  dram_ptr = (bpnode *)DFS(nvm_ptr);
}

char* subtree::DFS(nvmpage* root) {
    if(root == nullptr) {
        return nullptr;
    }
    TOID(nvmpage) nvm_node;
    nvm_node.oid.off = (uint64_t)root;
    nvmpage* nvm_node_ptr = D_RW(nvm_node);
    bpnode* node = new bpnode();
    
    int count = 0;
    
    node->hdr.is_deleted = nvm_node_ptr->hdr.is_deleted;
    node->hdr.last_index = nvm_node_ptr->hdr.last_index;
    node->hdr.level = nvm_node_ptr->hdr.level;
    node->hdr.switch_counter = nvm_node_ptr->hdr.switch_counter;
    
    node->hdr.leftmost_ptr = (bpnode *)DFS(nvm_node_ptr->hdr.leftmost_ptr);
    while(nvm_node_ptr->records[count].ptr != NULL) {
        node->records[count].key = nvm_node_ptr->records[count].key;
        if (nvm_node_ptr->hdr.leftmost_ptr != nullptr) {
            node->records[count].ptr = DFS(nvm_node_ptr->records[count].ptr);
            // sibling_ptr
            if (count == 0) {
              node->hdr.leftmost_ptr->hdr.sibling_ptr = (bpnode *)nvm_node_ptr->records[count].ptr;
            } else {
              bpnode* tmp = (bpnode *)nvm_node_ptr->records[count - 1].ptr;
              tmp->hdr.sibling_ptr = (bpnode *)nvm_node_ptr->records[count].ptr;
            }
        } else {
            node->records[count].ptr = nvm_node_ptr->records[count].ptr;
        }
        ++count;
    }

    node->records[count].ptr = nullptr;
    // sibling_ptr
    bpnode* tmp = (bpnode *)(node->records[node->hdr.last_index].ptr);
    if (node->hdr.leftmost_ptr != nullptr && tmp->hdr.leftmost_ptr != nullptr) {
        ((bpnode *)tmp->records[tmp->hdr.last_index].ptr)->hdr.sibling_ptr = (bpnode *)tmp->hdr.sibling_ptr->records[0].ptr;
    }
    return (char *)node;
}

void subtree::dram_to_nvm() {
  if (flag == false) {
    return ;
  }

  nvm_ptr = DFS((char *)dram_ptr);
  // delete log
}

char* subtree::DFS(char* root) {
    if(root == nullptr) {
        return nullptr;
    }
    TOID(nvmpage) nvm_node;
    POBJ_NEW(bt->pop, &nvm_node, nvmpage, NULL, NULL);
    D_RW(nvm_node)->constructor();
    nvmpage* nvm_node_ptr = D_RW(nvm_node);
    bpnode* node = (bpnode *)root;
    
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
            TOID(nvmpage) tmp;
            if (count == 0) {
              tmp.oid.off = (uint64_t)node->hdr.leftmost_ptr;
              D_RW(tmp)->hdr.sibling_ptr.oid.off = (uint64_t)node->records[count].ptr;
            } else {
              tmp.oid.off = (uint64_t)node->records[count - 1].ptr;
              D_RW(tmp)->hdr.sibling_ptr.oid.off = (uint64_t)node->records[count].ptr;
            }
            pmemobj_persist(pop, &(D_RW(tmp)->hdr), sizeof(D_RW(tmp)->hdr));
        } else {
            nvm_node_ptr->records[count].ptr = node->records[count].ptr;
        }
        ++count;
    }
    nvm_node_ptr->records[count].ptr = nullptr;
    pmemobj_persist(pop, nvm_node_ptr, sizeof(nvmpage));
    delete node;
    return (char *)nvm_node.oid.off;
}

void subtree::sync_subtree() {
  if (flag == false) {
    return ;
  }
}

// 子树高度增加需要分裂 热度减半
void subtree::split() {

}

// 合并 热度相加
void subtree::merge() {

}