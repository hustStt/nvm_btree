#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_common.h"
#include "nvm_bptree.h"
#include "random.h"

#define NODEPATH   "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"

using namespace scaledkv;


const uint64_t NVM_NODE_SIZE = 45 * (1ULL << 30);           // 45GB
const uint64_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB

void function_test(btree *bt, uint64_t ops);

int main(int argc, char *argv[]) {
    btree *bt = new btree();

    bt->Initial(NODEPATH, NVM_NODE_SIZE, VALUEPATH, NVM_VALUE_SIZE);

    // bt->PrintInfo();
    function_test(bt, 100);

    delete bt;
}

void function_test(btree *bt, uint64_t ops) {
    char valuebuf[NVM_ValueSize + 1];
    printf("******B+ tree function test start.******\n");
    rocksdb::Random rnd_put(0xdeadbeef); 
    for(uint64_t i = 0; i < ops; i ++) {
        auto key = rnd_put.Next();
        snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
        string value(valuebuf, NVM_ValueSize);
        bt->btree_insert(key, value);
    } 
    printf("******Insert test finished.******\n");
    bt->printAll();
    bt->PrintInfo();

}