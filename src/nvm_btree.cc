#include "nvm_btree.h"

NVMBtree::NVMBtree(PMEMobjpool *pool) {
    pop = pool;
    bt = new btree(pool); 
    if(!bt) {
        assert(0);
    }
    // bpnode *root = NewBpNode();
    // btree tmpbtree = btree(root);
}

NVMBtree::~NVMBtree() {
    if(bt) {
        delete bt;
    }
}
    
void NVMBtree::Insert(const unsigned long key, const string &value) {
    if(bt) {
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
        bt->btree_insert(key, pvalue);
    }
}

void NVMBtree::Insert(const unsigned long key, char *pvalue) {
    if(bt) {
        /*
        char* logvalue = log_alloc->AllocateAligned(18);
        char* tmp = logvalue;
        memset(logvalue, 0, 2);
        logvalue += 2;
        memcpy(logvalue, &key, 8);
        logvalue += 8;
        memcpy(logvalue, &pvalue, 8);
        clflush(tmp, 18);
        */
        bt->btreeInsert(key, pvalue);
        /*
        char *value = bt->btree_search(key);

        if((unsigned long)value != key) {
            printf("Not Found Get key %llx, pvalue %llx\n", key, pvalue);
        } 

        if(key == 0x509af66da8d1399dUL) {
            printf("Insert key %llx", key);
        }
        */
    }
}

void NVMBtree::Delete(const unsigned long key) {
    if(bt) {
        /*
        char *logvalue = log_alloc->AllocateAligned(2 + 8);
        char* tmp = logvalue;
        memset(logvalue, 1, 2);
        logvalue += 2;
        memcpy(logvalue, &key, 8);
        clflush(tmp, 10);
        */
        bt->btreeDelete(key);
    }
}

const string NVMBtree::Get(const unsigned long key) {
    char *pvalue = NULL;
    if(bt) {
        pvalue = bt->btreeSearch(key);
    }
    if(pvalue) {
        // print_log(LV_DEBUG, "Get pvalue is %p.", pvalue);
        return string(pvalue, NVM_ValueSize);
    }
    return "";
}

int NVMBtree::Get(const unsigned long key, char *&pvalue) {
    if(bt) {
        pvalue = bt->btreeSearch(key);
        //printf("Get key %llx, pvalue %llx\n", key, pvalue);
    }
    if(pvalue) {
        return 0;
    }
    return 1;

}

void NVMBtree::GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
    if(bt) {
        bt->btree_search_range(key1, key2, values, size);
    }
}

void NVMBtree::GetRange(unsigned long key1, unsigned long key2, void **pvalues, int &size) {
    if(bt) {
        bt->btreeSearchRange(key1, key2, pvalues, size);
    }
}

void NVMBtree::Print() {
    if(bt) {
        bt->printAll();
    }
}

void NVMBtree::PrintInfo() {
    if(bt) {
        bt->PrintInfo();
        //show_persist_data();
    }
}

void NVMBtree::FunctionTest(int ops) {

}

void NVMBtree::motivationtest() {
    
}