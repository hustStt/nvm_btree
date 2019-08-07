#include "nvm_bptree.h"

pthread_mutex_t print_mtx;

/*
 * class bpnode
 */
bool bpnode::remove(btree* bt, entry_key_t key, bool only_rebalance, bool with_lock) {
    hdr.mtx->lock();
    bool ret = remove_key(key);
    hdr.mtx->unlock();

    return ret;
}

bool bpnode::remove_rebalancing(btree* bt, entry_key_t key, bool only_rebalance, bool with_lock) {
    if(with_lock) {
        hdr.mtx->lock();
    }
    if(hdr.is_deleted) {
        if(with_lock) {
          hdr.mtx->unlock();
        }
        return false;
    }

    if(!only_rebalance) {
        register int num_entries_before = count();

        // This node is root
        if(this == (bpnode *)bt->root) {
            if(hdr.level > 0) {
                if(num_entries_before == 1 && !hdr.sibling_ptr) {
                    bt->root = (char *)hdr.leftmost_ptr;
                    // clflush((char *)&(bt->root), sizeof(char *));
                    hdr.Set_deleted(1);

                    // hdr.is_deleted = 1;
                }
            }

            // Remove the key from this node
            bool ret = remove_key(key);

            if(with_lock) {
                hdr.mtx->unlock();
            }
            return true;
        }

        bool should_rebalance = true;
        // check the node utilization
        if(num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) { 
            should_rebalance = false;
        }

        // Remove the key from this node
        bool ret = remove_key(key);

        if(!should_rebalance) {
            if(with_lock) {
                hdr.mtx->unlock();
            }
            return (hdr.leftmost_ptr == NULL) ? ret : true;
        }
    } 

    //Remove a key from the parent node
    entry_key_t deleted_key_from_parent = 0;
    bool is_leftmost_node = false;
    bpnode *left_sibling;
    bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
        &deleted_key_from_parent, &is_leftmost_node, &left_sibling);

    if(is_leftmost_node) {
        if(with_lock) {
            hdr.mtx->unlock();
        }

        if(!with_lock) {
            hdr.sibling_ptr->hdr.mtx->lock();
        }
        hdr.sibling_ptr->remove(bt, hdr.sibling_ptr->records[0].key, true, with_lock);
        if(!with_lock) {
            hdr.sibling_ptr->hdr.mtx->unlock();
        }
        return true;
    }

    if(with_lock) {
        left_sibling->hdr.mtx->lock();
    }

    while(left_sibling->hdr.sibling_ptr != this) {
        if(with_lock) {
            bpnode *t = left_sibling->hdr.sibling_ptr;
            left_sibling->hdr.mtx->unlock();
            left_sibling = t;
            left_sibling->hdr.mtx->lock();
        }
        else
            left_sibling = left_sibling->hdr.sibling_ptr;
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
                    insert_key(left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
                } 
                left_sibling->Set_Ptr(m, nullptr);
                left_sibling->hdr.Set_last_index(m-1);

                // left_sibling->records[m].ptr = nullptr;
                // clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

                // left_sibling->hdr.last_index = m - 1;
                // clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));

                parent_key = records[0].key; 
            }
            else{
                insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr, &num_entries); 

                for(int i=left_num_entries - 1; i>m; i--){
                    insert_key(left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
                }

                parent_key = left_sibling->records[m].key; 

                hdr.Set_leftmost((bpnode *)(left_sibling->records[m].ptr));
                left_sibling->Set_Ptr(m, nullptr);
                left_sibling->hdr.Set_last_index(m-1);

                // hdr.leftmost_ptr = (bpnode*)left_sibling->records[m].ptr; 
                // clflush((char *)&(hdr.leftmost_ptr), sizeof(bpnode *));

                // left_sibling->records[m].ptr = nullptr;
                // clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

                // left_sibling->hdr.last_index = m - 1;
                // clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));
            }

            if(left_sibling == ((bpnode *)bt->root)) {
                bpnode* new_root = new (bt->AllocNode())bpnode(left_sibling, parent_key, this, hdr.level + 1);
                // bpnode* new_root = new bpnode(left_sibling, parent_key, this, hdr.level + 1);
                bt->setNewRoot((char *)new_root);
            }
            else {
                bt->btree_insert_internal((char *)left_sibling, parent_key, (char *)this, hdr.level + 1);
            }
        }
        else{ // from leftmost case
            hdr.Set_deleted(1);
            // hdr.is_deleted = 1;
            // clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));
            bpnode* new_sibling = new (bt->AllocNode())bpnode(hdr.level);
            // bpnode* new_sibling = new bpnode(hdr.level); 
            new_sibling->hdr.mtx->lock();
            new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

            int num_dist_entries = num_entries - m;
            int new_sibling_cnt = 0;

            if(hdr.leftmost_ptr == nullptr){
                for(int i=0; i<num_dist_entries; i++){
                    left_sibling->insert_key(records[i].key, records[i].ptr, &left_num_entries); 
                } 

                for(int i=num_dist_entries; records[i].ptr != NULL; i++){
                    new_sibling->insert_key(records[i].key, records[i].ptr, &new_sibling_cnt, false); 
                } 

                new_sibling->Persistent();
                // clflush((char *)(new_sibling), sizeof(bpnode)); 

                left_sibling->hdr.Set_sibling(new_sibling);
                // left_sibling->hdr.sibling_ptr = new_sibling;
                // clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(bpnode *));

                parent_key = new_sibling->records[0].key; 
            }
            else{
                left_sibling->insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr, &left_num_entries);

                for(int i=0; i<num_dist_entries - 1; i++){
                    left_sibling->insert_key(records[i].key, records[i].ptr,
                        &left_num_entries); 
                } 

                parent_key = records[num_dist_entries - 1].key;

                new_sibling->hdr.leftmost_ptr = (bpnode*)records[num_dist_entries - 1].ptr;
                for(int i=num_dist_entries; records[i].ptr != NULL; i++){
                    new_sibling->insert_key(records[i].key, records[i].ptr,
                        &new_sibling_cnt, false); 
                } 
                new_sibling->Persistent();

                // clflush((char *)(new_sibling), sizeof(bpnode));

                left_sibling->hdr.Set_sibling(new_sibling);
                // left_sibling->hdr.sibling_ptr = new_sibling;
                // clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(bpnode *));
            }

            if(left_sibling == ((bpnode *)bt->root)) {
                bpnode* new_root = new (bt->AllocNode())bpnode(left_sibling, parent_key, new_sibling, hdr.level + 1);
                // bpnode* new_root = new bpnode(left_sibling, parent_key, new_sibling, hdr.level + 1);
                bt->setNewRoot((char *)new_root);
            }
            else {
                bt->btree_insert_internal((char *)left_sibling, parent_key, (char *)new_sibling, hdr.level + 1);
            }

            new_sibling->hdr.mtx->unlock();
        }
    }
    else {
        hdr.Set_deleted(1);
        // hdr.is_deleted = 1;
        // clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

        if(hdr.leftmost_ptr)
            left_sibling->insert_key(deleted_key_from_parent, (char *)hdr.leftmost_ptr, &left_num_entries);

        for(int i = 0; records[i].ptr != NULL; ++i) { 
            left_sibling->insert_key(records[i].key, records[i].ptr, &left_num_entries);
        }

        left_sibling->hdr.Set_sibling(hdr.sibling_ptr);
        // left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
        // clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(bpnode *));
    }

    if(with_lock) {
        left_sibling->hdr.mtx->unlock();
        hdr.mtx->unlock();
    }

    return true;
}

bpnode * bpnode::store(btree* bt, char* left, entry_key_t key, char* right,
            bool flush, bool with_lock, bpnode *invalid_sibling) {
    if(with_lock) {
        hdr.mtx->lock(); // Lock the write lock
    }
    if(hdr.is_deleted) {
        if(with_lock) {
            hdr.mtx->unlock();
        }
        return NULL;
    }

    // If this node has a sibling node,
    if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
        // Compare this key with the first key of the sibling
        if(key > hdr.sibling_ptr->records[0].key) {
            if(with_lock) { 
                hdr.mtx->unlock(); // Unlock the write lock
            }
            return hdr.sibling_ptr->store(bt, NULL, key, right, true, with_lock, invalid_sibling);
        }
    }

    register int num_entries = count();

    // FAST
    if(num_entries < cardinality - 1) {
        insert_key(key, right, &num_entries, flush);

        if(with_lock) {
            hdr.mtx->unlock(); // Unlock the write lock
        }

        return this;
    }
    else {// FAIR
        // overflow
        // create a new node
        bpnode* sibling = new (bt->AllocNode())bpnode(hdr.level);
        // bpnode* sibling = new bpnode(hdr.level); 
        register int m = (int) ceil(num_entries/2);
        entry_key_t split_key = records[m].key;

        // migrate half of keys into the sibling
        int sibling_cnt = 0;
        if(hdr.leftmost_ptr == NULL){ // leaf node
            for(int i=m; i<num_entries; ++i){ 
                sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
        }
        else{ // internal node
            for(int i=m+1;i<num_entries;++i){ 
                sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
            sibling->hdr.leftmost_ptr = (bpnode*) records[m].ptr;
        }
        sibling->hdr.Set_sibling(hdr.sibling_ptr);
        hdr.Set_sibling(sibling);
        // sibling->hdr.sibling_ptr = hdr.sibling_ptr;
        // clflush((char *)sibling, sizeof(bpnode));

        // hdr.sibling_ptr = sibling;
        // clflush((char*) &hdr, sizeof(hdr));

        // set to NULL
        if(IS_FORWARD(hdr.switch_counter)) {
            hdr.Set_switch_counter(hdr.switch_counter + 2);
            // hdr.switch_counter += 2;
        }
        else {
            hdr.Set_switch_counter(hdr.switch_counter + 1);
            // ++hdr.switch_counter;
        }

        Set_Ptr(m, nullptr);
        hdr.Set_last_index(m - 1);
        // records[m].ptr = NULL;
        // clflush((char*) &records[m], sizeof(entry));

        // hdr.last_index = m - 1;
        // clflush((char *)&(hdr.last_index), sizeof(int16_t));

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

        // Set a new root or insert the split key to the parent
        if(bt->root == (char *)this) { // only one node can update the root ptr
            bpnode* new_root = new (bt->AllocNode())bpnode((bpnode*)this, split_key, sibling, hdr.level + 1);
            // bpnode* new_root = new bpnode((bpnode*)this, split_key, sibling, hdr.level + 1);
            bt->setNewRoot((char *)new_root);

            if(with_lock) {
                hdr.mtx->unlock(); // Unlock the write lock
            }
        }
        else {
            if(with_lock) {
                hdr.mtx->unlock(); // Unlock the write lock
            }
            bt->btree_insert_internal(NULL, split_key, (char *)sibling, hdr.level + 1);
        }
        return ret;
    }

}

char *bpnode::linear_search(entry_key_t key) {
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

        if((t = (char *)hdr.sibling_ptr) && key >= ((bpnode *)t)->records[0].key)
            return t;

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

        if((t = (char *)hdr.sibling_ptr) != NULL) {
            if(key >= ((bpnode *)t)->records[0].key)
            return t;
        }

        if(ret) {
            return ret;
        }
        else
            return (char *)hdr.leftmost_ptr;
    }

    return NULL;

}
void bpnode::linear_search_range(entry_key_t min, entry_key_t max, unsigned long *buf) {
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

        current = current->hdr.sibling_ptr;
    }
}

void bpnode::print() {
    if(hdr.leftmost_ptr == NULL) 
        printf("[%d] leaf %p \n", this->hdr.level, this);
    else 
        printf("[%d] internal %p \n", this->hdr.level, this);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if(IS_FORWARD(hdr.switch_counter))
        printf("->\n");
    else
        printf("<-\n");

    if(hdr.leftmost_ptr!=NULL) 
        printf("%p ",hdr.leftmost_ptr);

    for(int i=0;records[i].ptr != NULL;++i)
        printf("%ld,%p ",records[i].key,records[i].ptr);

    printf("%p ", hdr.sibling_ptr);

    printf("\n");
}

void bpnode::printAll() {
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

/*
 * class btree
 */
btree::btree(){
    // root = (char*)new bpnode();
    node_alloc = nullptr;
    value_alloc = nullptr;
    root = nullptr;
    height = 1;
}

btree::~btree() {
    if(node_alloc != nullptr) {
        delete node_alloc;
    }

    if(value_alloc != nullptr) {
        delete value_alloc;
    }
}

void btree::Initial(const std::string &nodepath, uint64_t nodesize, 
            const std::string &valuepath, uint64_t valuesize) {
    node_alloc = new NVMAllocator(nodepath, nodesize);
    if(node_alloc == nullptr) {
        assert(0);
    }
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete node_alloc;
        node_alloc = nullptr;
        assert(0);
    } 
    root = (char *)(new (AllocNode())bpnode());
}

char *btree::AllocNode() {
    if(node_alloc ==  nullptr) {
        return nullptr;
    }
    return node_alloc->Allocate(sizeof(bpnode));
}

void btree::setNewRoot(char *new_root) {
    this->root = (char*)new_root;
    // clflush((char*)&(this->root),sizeof(char*));
    ++height;
}

std::string& btree::btree_search(entry_key_t key) {
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

    if(!t || (char *)t != (char *)key) {
        printf("NOT FOUND %lu, t = %x\n", key, t);
        return std::string("", 0);
    }

    return std::string(t, NVM_ValueSize);
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, const std::string &value);//need to be string
    bpnode* p = (bpnode*)root;
    char *pvalue = value_alloc->Allocate(value.size());
    pmem_memcpy_persist(pvalue, value.c_str(), value.size());

    while(p->hdr.leftmost_ptr != NULL) {
        p = (bpnode*)p->linear_search(key);
    }

    if(!p->store(this, NULL, key, pvalue, true, true)) { // store 
        btree_insert(key, pvalue);
    }
}

// store the key into the node at the given level 
void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level) {
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

    if(p) {
        if(!p->remove(this, key)) {
            btree_delete(key);
        }
    }
    else {
        printf("not found the key to delete %lu\n", key);
    }
}

void btree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
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

void btree::printAll(){
    pthread_mutex_lock(&print_mtx);
    int total_keys = 0;
    bpnode *leftmost = (bpnode *)root;
    printf("root: %p\n", root);
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