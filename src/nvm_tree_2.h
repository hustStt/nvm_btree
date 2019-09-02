#ifndef NV_TREE_2_H
#define NV_TREE_2_H

//#define CONDITIONAL_WRITE

#include <iostream>
#include <algorithm>
#include <tbb/spin_rw_mutex.h>

#include "util.h"
#include "index.h"
#include "threadinfo.h"
#include "nvm_mgr.h"

namespace nvindex{
namespace NV_tree
{

static const int LN_SIZE = 64;

struct node
{
    uint64_t key;
    void * data;
    bool flag;
    node(uint64_t k, void * d) : key(k), data(d) {}
    node(uint64_t k) : key(k) {}
    node() {}
};

class Node
{
  public:
    virtual bool isLeaf() = 0;
};

template <typename uint64_t, typename void *, int size>
class LN : public Node<uint64_t, void *, size>
{
    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;

  public:
    bool isLeaf()
    {
        return true;
    }

    LN()
    {
        entry = 0;
        persist_entry = 0;
        version = 0;
    }

    node<uint64_t, void *> data[64];
    int entry;
    int persist_entry;

    /*
     *  /-------- version ------- / perserved / splitting / updating /
     *  /-------- 48 ------------ /    14     /      1    /    1     /
     */
    volatile uint64_t version;
    static const uint64_t LOCK_MASK = 1llu;
    static const uint64_t SPLIT_MASK = 2llu;
    static const int VERSION_SHIFT = 16;
    static const uint64_t META_MASK = (1 << VERSION_SHIFT) - 1;

    inline uint64_t get_version()
    {
        return version >> VERSION_SHIFT;
    }
    inline uint64_t set_version(uint64_t _v)
    {
        version = (_v << VERSION_SHIFT) | (_v & META_MASK);
    }

    inline uint64_t unlock_version(uint64_t _v)
    {
        return _v & (~LOCK_MASK);
    }
    inline uint64_t locked_version(uint64_t _v)
    {
        return _v | LOCK_MASK;
    }
    inline uint64_t stable_version()
    {
        uint64_t _v = version;
        while (_v & LOCK_MASK)
        {
            _v = version;
        }
        return _v;
    }

    void lock()
    {
        uint64_t _v = version;
        while (!__sync_bool_compare_and_swap(&version, unlock_version(_v), locked_version(_v)))
        {
            _v = version;
        }
    }

    void unlock(bool add_version = true)
    {
        if (add_version)
            set_version(get_version() + 1);
        version &= (~LOCK_MASK);
    }

    LN<uint64_t, void *, size> *next;

    inline void flush()
    {
        flush_data(data, sizeof(node<uint64_t, void *>) * 64);
    }

    inline void _prefetch(){
        char * start_ptr =(char*)this;
        int length = (sizeof(LN<uint64_t, void *, size>))/64;
        while(length-- > 0){
            prefetch(start_ptr);
            start_ptr += 64;
        }
    }

} __attribute__((aligned(64)));

template <typename uint64_t, typename void *, int size>
class IN : public Node<uint64_t, void *, size>
{
  public:
    typedef Node<uint64_t, void *, size> node_t;
    typedef IN<uint64_t, void *, size> inner_node_t;

  public:
    alignas(64) uint64_t keys[size];
    uint64_t lower_bound;
    uint64_t upper_bound;
    bool infinite_lower_bound;
    bool infinite_upper_bound;

    node_t *children_ptrs[size];
    inner_node_t *parent;
    int children_number;

    IN()
    {
        infinite_lower_bound = infinite_upper_bound = true;
        parent = nullptr;
    }

    inline int binary_search(uint64_t key)
    {
        int l = 0, r = children_number - 1;
        while (l < r)
        {
            int mid = (l + r) / 2;
            if (keys[mid] >= key)
            {
                r = mid;
            }
            else
            {
                l = mid + 1;
            }
        }
        return l;
    }

    bool isLeaf() { return false; }

    node_t *find(uint64_t key)
    {
        return children_ptrs[binary_search(key)];
    }

    inline bool contains(uint64_t key)
    {
        if (!infinite_lower_bound && key < lower_bound)
        {
            return false;
        }
        if (!infinite_upper_bound && key >= upper_bound)
        {
            return false;
        }
        return true;
    }

    node_t *find_pre(uint64_t key)
    {
        int k = binary_search(key);
        if (k == 0)
            return NULL;
        return children_ptrs[k - 1];
    }

    bool insert(uint64_t key, node_t *child)
    {
        int d = binary_search(key);
        if (keys[d] == key)
        {
            assert(d >= children_number - 1);
        }
        for (int i = children_number - 1; i > d; i--)
        {
            keys[i] = keys[i - 1];
            children_ptrs[i + 1] = children_ptrs[i];
        }
        keys[d] = key;
        children_ptrs[d + 1] = child;
        assert(d + 1 < size);
        children_number++;

        if (children_number == size)
        {
            return true;
        }
        return false;
    }
} __attribute__((aligned(64)));

class Btree : public Index<uint64_t, void *, size>
{
    typedef Node<uint64_t, void *, size> node_t;
    typedef IN<uint64_t, void *, size> inner_node_t;
    typedef LN<uint64_t, void *, size> leaf_node_t;
    typedef node<uint64_t, void *> item_t;

    std::pair<leaf_node_t *, inner_node_t *> find_leaf(uint64_t key)
    {
        if (root->isLeaf())
        {
            return std::make_pair((leaf_node_t *)root, (inner_node_t *)NULL);
        }

        inner_node_t *parent = (inner_node_t *)root;
        node_t *child = parent->find(key);

        while (!child->isLeaf())
        {
            parent = (inner_node_t *)child;
            child = parent->find(key);
        }

        return std::make_pair((leaf_node_t *)child, parent);
    }

    node_t *root;

    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;
    speculative_lock_t mtx;

    void htmTraverseLeaf(uint64_t key, inner_node_t *&parent, leaf_node_t *&leaf)
    {

        parent = nullptr;
        node_t *child = root;

        while (!child->isLeaf())
        {
            parent = (inner_node_t *)child;
            child = parent->find(key);
            //std::cout << "[Traverse] " << key << " " << child << std::endl;
        }
        leaf = (leaf_node_t *)child;
    }

    bool htmLeafUpdateSlot(leaf_node_t *leaf, uint64_t key, int entry)
    {
        return leaf->update_slot(key, entry);
    }

    node_t *split_inner_node(inner_node_t *node)
    {
        inner_node_t *successor = new inner_node_t;
        successor->children_number = node->children_number = size / 2;
        for (int i = 0; i < size / 2; i++)
        {
            successor->children_ptrs[i] = node->children_ptrs[i + size / 2];
            if (successor->children_ptrs[i]->isLeaf() == false)
            {
                ((inner_node_t *)(successor->children_ptrs[i]))->parent = successor;
            }
            successor->keys[i] = node->keys[i + size / 2];
        }

        successor->parent = node->parent;
        successor->lower_bound = node->keys[size / 2 - 1];
        successor->infinite_lower_bound = false;
        successor->upper_bound = node->upper_bound;
        successor->infinite_upper_bound = node->infinite_upper_bound;

        node->upper_bound = successor->lower_bound;
        node->infinite_upper_bound = false;

        return successor;
    }

    void insertInnerNode(inner_node_t *parent, leaf_node_t *leaf, leaf_node_t *newleaf, uint64_t sep)
    {
        bool needsplit = true;
        node_t *successor = newleaf;
        node_t *child = leaf;

        while (needsplit)
        {
            if (parent == nullptr)
            {
                assert(child == root);
                inner_node_t *newroot = new inner_node_t;
                newroot->keys[0] = sep;
                newroot->children_ptrs[0] = child;
                newroot->children_ptrs[1] = successor;
                newroot->children_number = 2;

                if (child->isLeaf() == false)
                {
                    ((inner_node_t *)child)->parent = newroot;
                    ((inner_node_t *)successor)->parent = newroot;
                }
                root = newroot;
                std::cout << "new root " << root << std::endl;
                return;
            }
            needsplit = parent->insert(sep, successor);
            if (needsplit)
            {
                successor = split_inner_node(parent);
                child = parent;
                sep = parent->keys[size / 2 - 1];
            }
            parent = parent->parent;
        }
    }

    void TreeUpdate(inner_node_t *parent, leaf_node_t *leaf, leaf_node_t *next, uint64_t sep)
    {
        if (parent == nullptr || parent->contains(sep))
        {
            insertInnerNode(parent, leaf, next, sep);
        }
        else
        {
            leaf_node_t *nleaf;
            //TODO: 更精确的函数
            htmTraverseLeaf(sep, parent, nleaf);

            assert(nleaf == leaf);
            assert(parent->contains(sep));
            insertInnerNode(parent, leaf, next, sep);
        }
    }

    void generateNextLeaf(leaf_node_t *leaf, uint64_t &sep)
    {
        // 1. log leaf，创建新的leaf 和 nextleaf。
        leaf_node_t *next = new (alloc_leaf()) leaf_node_t;
        leaf_node_t *log = new leaf_node_t;
        memcpy(log, leaf, sizeof(leaf_node_t));
        log->flush();

        //
        int split = log->entry / 2;

        leaf->persist_entry = leaf->entry = split;
        next->persist_entry = next->entry = log->entry - split;

        for (int i = 0; i < next->entry; i++)
        {
            next->data[i] = log->data[i + split];
        }
        next->next = leaf->next;
        leaf->next = next;
        sep = leaf->data[split - 1].key;

        leaf->flush();
        next->flush();
//        std::cout << "Entry " << leaf->entry << " " << next->entry << std::endl;
        delete log;
    }

    void shrinkLeaf(leaf_node_t *leaf)
    {
    }

    void splitLeafNode(leaf_node_t *leaf, inner_node_t *parent)
    {

        std::map<uint64_t, std::pair<void *, bool>> maps;
        for (int i = leaf->entry-1; i >= 0; i--)
        {
            if (maps.find(leaf->data[i].key) == maps.end())
            {
                maps.insert(std::make_pair(leaf->data[i].key,
                                           std::make_pair(leaf->data[i].data, leaf->data[i].flag)));
            }
        }
        leaf_node_t *tmp = leaf;
        tmp->entry = 0;
        for (auto it : maps)
        {
            tmp->data[tmp->entry].key = it.first;
            tmp->data[tmp->entry].data = it.second.first;
            tmp->data[tmp->entry].flag = it.second.second;
            tmp->entry++;
        }

        if (maps.size() > LN_SIZE / 2)
        {
            // split
            uint64_t sep;
            generateNextLeaf(tmp, sep);
            TreeUpdate(parent, leaf, leaf->next, sep);
        }
        else
        {
            shrinkLeaf(leaf);
            leaf->flush();
        }
    }
    leaf_node_t* anchor;
  public:

    Btree()
    {
        void* thread_info;
        int threads;
        bool safe;
        bool init = init_nvm_mgr(thread_info, threads, safe);
        register_threadinfo();

        if (init){
            set_leaf_size(sizeof(leaf_node_t));
            anchor = new (alloc_leaf()) leaf_node_t;
            assert(anchor);
            root = anchor;
            printf("new NVTree\n");
        }else{
            assert(0);
        }
    }

    bool modify(uint64_t key, void * value, bool remove = false, bool update=false)
    {
        speculative_lock_t::scoped_lock lock;
        inner_node_t *parent;
        leaf_node_t *leaf;

        htmTraverseLeaf(key, parent, leaf);
        int entry = leaf->entry;
        #ifdef CONDITIONAL_WRITE
        bool exists = false;
        for(int i=entry-1; i>=0; i--){
            if (leaf->data[i].key == key){
                if (leaf->data[i].flag){
                    exists = true;
                }
                break;
            }
        }
        if (exists != update){
            return false;
        }
        #endif

        leaf->data[entry].key = key;
        leaf->data[entry].data = value;
        leaf->data[entry].flag = (remove==false);
        flush_data(&leaf->data[entry], sizeof(node<uint64_t, void *>));

        leaf->entry++;
        flush_data(&leaf->entry, sizeof(int));

        if (leaf->entry == LN_SIZE)
        {
            //std::cout << "Require split\n";
            splitLeafNode(leaf, parent);
        }
        assert(leaf->entry < LN_SIZE);
        return true;
    }

    bool insert(uint64_t key, void * value)
    {
        return modify(key, value, false);
    }

    void scan(uint64_t key, bool (*function)(uint64_t key, void * value))
    {
        leaf_node_t* leaf;
        inner_node_t* parent;
        htmTraverseLeaf(key, parent, leaf);

        item_t tmps[LN_SIZE];

        while (leaf){
            memcpy(tmps, leaf->data, sizeof(item_t)*leaf->entry);
            std::sort(tmps, tmps+leaf->entry, [](item_t& n1, item_t& n2){
                if (n1.flag < n2.flag){
                    return true;
                }else if (n1.flag == n2.flag){
                    return n1.key < n2.key;
                }
                return false;
            });
            uint64_t last_key = -1;

            for (int i=0; i<leaf->entry; i++)
            {
                if (tmps[i].flag && tmps[i].key != last_key && tmps[i].key >= key){
                    if ((*function)(tmps[i].key, tmps[i].data) == true){
                        return;
                    }
                    last_key = tmps[i].key;
                }
            }
            leaf = leaf->next;
        }
    }


    void * get(uint64_t key, double latency_breaks[3])
    {
        inner_node_t *parent;
        leaf_node_t *leaf;

        htmTraverseLeaf(key, parent, leaf);

        for (int i = leaf->entry - 1; i >= 0; i--)
        {
            if (leaf->data[i].key == key)
            {
                if (leaf->data[i].flag)
                    return leaf->data[i].data;
                else
                    return void *(-1);
            }
        }
        return void *(-1);
    }

    bool update(uint64_t key, void * value, double latency_breaks[3])
    {
        return modify(key, value, false, true);
    }

    bool remove(uint64_t key)
    {
        return modify(key, void *(0), true);
    }


    void rebuild()
    {
        leaf_node_t* leaf = anchor;
        if (leaf->next == nullptr){
            root = leaf;
            return;
        }

        inner_node_t* parent = nullptr;
        int total_leaves = 1;
        while(leaf){
            leaf = leaf->next;
            total_leaves++;
        }
        printf("total level nodes: %d", total_leaves);

        uint64_t* seps = new uint64_t[total_leaves];
        node_t** nodes = new node_t*[total_leaves];

        leaf = anchor;
        int i = 0;
        while(leaf){
            uint64_t max_sep = uint64_t(-1);
            for (int j=0; j<leaf->entry; j++){
                if (leaf->data[j].key > max_sep){
                    max_sep = leaf->data[j].key;
                }
            }
            seps[i] = max_sep;
            nodes[i] = leaf;
            leaf = leaf->next;
        }

        int current_level_size = total_leaves;
        while(current_level_size > 1){
            int parent_size = (current_level_size+size-1)/size;
            inner_node_t* inner_nodes = new inner_node_t[parent_size];
            for(int i=0; i<parent_size; i++){
                for (int j=0; j<size && i*size + j < current_level_size; j++){
                    inner_nodes[i].keys[j] = seps[i*size + j];
                    inner_nodes[i].children_ptrs[j] = nodes[i*size+j];
                }
                inner_nodes[i].children_number = std::min(size, current_level_size-i*size);
                seps[i] = inner_nodes[i].keys[inner_nodes[i].children_number-1];
                nodes[i] = &inner_nodes[i];
            }
            current_level_size = parent_size;
        }
    }
};
} // namespace NV_tree2

} // nvindex
#endif
