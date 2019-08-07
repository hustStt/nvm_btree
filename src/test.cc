#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_bptree.h"
#include "random.h"

#define NODEPATH   "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"

const uint64_t NVM_NODE_SIZE = 45 * (1ULL << 30);           // 45GB
const uint64_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB


int main(int argc, char *argv[]) {
    btree *bt = new btree();

    bt->Initial(NODEPATH, NVM_NODE_SIZE, VALUEPATH, NVM_VALUE_SIZE);

    // bt->PrintInfo();
    function_test(100);

    delete bt;
}

void function_test(btree *bt, uint64_t ops) {
    printf("******B+ tree function test start.******\n");
    rocksdb::Random rnd_put(0xdeadbeef); 
    for(uint64_t i; i < ops; i ++) {
        auto key = rnd_put.Next();
        bt->btree_insert(key, (char*) keys);
    } 
    printf("******Insert test finished.******\n");
    bt->printAll();
    bt->PrintInfo();

}