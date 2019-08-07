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

    rocksdb::Random rnd_get(0xdeadbeef);
    for(uint64_t i = 0; i < ops; i ++) {
        memset(valuebuf, 0, sizeof(valuebuf));
        // snprintf(keybuf, sizeof(keybuf), "%07d", i);
        auto key = rnd_get.Next();
        snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
        string value(valuebuf, NVM_ValueSize);
        const string tmp_value = bt->btree_search(key);
        printf("value size is %d\n", tmp_value.size());
        if(tmp_value.size() == 0) {
            printf("Error: Get key-value %lld faild.(key:%llx)\n", i, key);
        } else if(strncmp(value.c_str(), tmp_value.c_str(), NVM_ValueSize) != 0) {
            printf("Error: Get %llx key-value faild.(Expect:%s, but Get %s)\n", key, value.c_str(), tmp_value.c_str());
        }
    }
    printf("******Get test finished.*****\n");
    printf("******B+ tree function test finished.******\n");
    bt->printAll();
    bt->PrintInfo();

}