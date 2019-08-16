
#include "con_btree.h"

pthread_mutex_t print_mtx;

/*
 * class btree
 */
btree::btree(){
  root = (char*)new bpnode();
  height = 1;
}

void btree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  clflush((char*)&(this->root),sizeof(char*));
  ++height;
}

char *btree::btree_search(entry_key_t key){
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode *)p->linear_search(key);
  }

  bpnode *t;
  while((t = (bpnode *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p) {
      break;
    }
  }

  if(!t) {
    printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char* right){ //need to be string
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode*)p->linear_search(key);
  }

  if(!p->store(this, NULL, key, right, true, true)) { // store 
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

  if(!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key) {
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL){
    p = (bpnode*) p->linear_search(key);
  }

  bpnode *t;
  while((t = (bpnode *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p)
      break;
  }

  if(p && t) {
    if(!p->remove(this, key)) {
      btree_delete(key);
    }
  }
  else {
    printf("not found the key to delete %lu\n", key);
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

  p->hdr.mtx->lock();

  if((char *)p->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    p->hdr.mtx->unlock();
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

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min, entry_key_t max, unsigned long *buf) {
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

void btree::btree_search_range(entry_key_t min, entry_key_t max, 
        std::vector<std::string> &values, int &size) {

}

void btree::printAll(){
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  bpnode *leftmost = (bpnode *)root;
  printf("root: %x\n", root);
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

  printf("total number of keys: %d\n", total_keys);
  pthread_mutex_unlock(&print_mtx);
}


void btree::PrintInfo() {
    printf("This is a b+ tree.\n");
    printf("Node size is %lu, M path is %d.\n", sizeof(bpnode), cardinality);
    printf("Tree height is %d.\n", height);

}