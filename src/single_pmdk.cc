#include "single_pmdk.h"

/*
 * class nvmbtree
 */
void nvmbtree::constructor(PMEMobjpool *pool) {
  pop = pool;
  POBJ_NEW(pop, &root, nvmpage, NULL, NULL);
  D_RW(root)->constructor();
  height = 1;
}

void nvmbtree::setNewRoot(TOID(nvmpage) new_root) {
  root = new_root;
  pmemobj_persist(pop, &root, sizeof(TOID(nvmpage)));
  ++height;
}

char *nvmbtree::btree_search(entry_key_t key) {
  TOID(nvmpage) p = root;

  while (D_RO(p)->hdr.leftmost_ptr != NULL) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  uint64_t t = (uint64_t)D_RW(p)->linear_search(key);
  /*
  while ((t = (uint64_t)D_RW(p)->linear_search(key)) ==
         D_RO(p)->hdr.sibling_ptr.oid.off) {
    p.oid.off = t;
    printf("sibling_ptr search\n");
    if (!t) {
      break;
    }
  }
sc*/
  if (!t) {
    printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

// insert the key in the leaf node
void nvmbtree::btree_insert(entry_key_t key, char *right) {
  TOID(nvmpage) p = root;

  while (D_RO(p)->hdr.leftmost_ptr != NULL) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  if (!D_RW(p)->store(this, NULL, key, right, true)) { // store
    btree_insert(key, right);
  }
}

// store the key into the node at the given level
void nvmbtree::btree_insert_internal(char *left, entry_key_t key, char *right,
                                  uint32_t level) {
  if (level > D_RO(root)->hdr.level)
    return;

  TOID(nvmpage) p = root;

  while (D_RO(p)->hdr.level > level)
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);

  if (!D_RW(p)->store(this, NULL, key, right, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void nvmbtree::btree_delete(entry_key_t key) {
  TOID(nvmpage) p = root;

  while (D_RO(p)->hdr.leftmost_ptr != NULL) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  uint64_t t = (uint64_t)(D_RW(p)->linear_search(key));
  /*
  while ((t = (uint64_t)(D_RW(p)->linear_search(key))) ==
         D_RW(p)->hdr.sibling_ptr.oid.off) {
    p.oid.off = t;
    printf("sibling_ptr delete\n");
    if (!t)
      break;
  }
  */
  if (t) {
    if (!D_RW(p)->remove(this, key)) {
      btree_delete(key);
    }
  } else {
    printf("not found the key to delete %lu\n", key);
  }
}

void nvmbtree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level,
                                  entry_key_t *deleted_key,
                                  bool *is_leftmost_node, nvmpage **left_sibling) {
  if (level > D_RO(root)->hdr.level)
    return;

  TOID(nvmpage) p = root;

  while (D_RW(p)->hdr.level > level) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  if ((char *)D_RO(p)->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    return;
  }

  *is_leftmost_node = false;

  for (int i = 0; D_RO(p)->records[i].ptr != NULL; ++i) {
    if (D_RO(p)->records[i].ptr == ptr) {
      if (i == 0) {
        if ((char *)D_RO(p)->hdr.leftmost_ptr != D_RO(p)->records[i].ptr) {
          *deleted_key = D_RO(p)->records[i].key;
          *left_sibling = D_RO(p)->hdr.leftmost_ptr;
          D_RW(p)->remove(this, *deleted_key, false, false);
          break;
        }
      } else {
        if (D_RO(p)->records[i - 1].ptr != D_RO(p)->records[i].ptr) {
          *deleted_key = D_RO(p)->records[i].key;
          *left_sibling = (nvmpage *)D_RO(p)->records[i - 1].ptr;
          D_RW(p)->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }
}

// Function to search keys from "min" to "max"
void nvmbtree::btree_search_range(entry_key_t min, entry_key_t max,
                               unsigned long *buf) {
  TOID(nvmpage) p = root;

  while (p.oid.off != 0) {
    if (D_RO(p)->hdr.leftmost_ptr != NULL) {
      // The current nvmpage is internal
      p.oid.off = (uint64_t)D_RW(p)->linear_search(min);
    } else {
      // Found a leaf
      D_RW(p)->linear_search_range(min, max, buf);

      break;
    }
  }
}

void nvmbtree::printAll() {
  int total_keys = 0;
  TOID(nvmpage) leftmost = root;
  printf("root: %x\n", root.oid.off);
  if (root.oid.off) {
    do {
      TOID(nvmpage) sibling = leftmost;
      while (sibling.oid.off) {
        if (D_RO(sibling)->hdr.level == 0) {
          total_keys += D_RO(sibling)->hdr.last_index + 1;
        }
        D_RW(sibling)->print();
        sibling = D_RO(sibling)->hdr.sibling_ptr;
      }
      printf("-----------------------------------------\n");
      leftmost.oid.off = (uint64_t)D_RO(leftmost)->hdr.leftmost_ptr;
    } while (leftmost.oid.off != 0);
  }

  printf("total number of keys: %d\n", total_keys);
}

void nvmbtree::randScounter() {
  TOID(nvmpage) leftmost = root;
  srand(time(NULL));
  if (root.oid.off) {
    do {
      TOID(nvmpage) sibling = leftmost;
      while (sibling.oid.off) {
        D_RW(sibling)->hdr.switch_counter = rand() % 100;
        sibling = D_RO(sibling)->hdr.sibling_ptr;
      }
      leftmost.oid.off = (uint64_t)D_RO(leftmost)->hdr.leftmost_ptr;
    } while (leftmost.oid.off != 0);
  }
}