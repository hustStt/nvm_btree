#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_common.h"
#include "nvm_bptree.h"
#include "random.h"
#include "debug.h"

#define NODEPATH   "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"

using namespace scaledkv;


const uint64_t NVM_NODE_SIZE = 45 * (1ULL << 30);           // 45GB
const uint64_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB
const uint64_t MAX_KEY = ~(0ULL);

int using_existing_data = 0;
int test_type = 1;
uint64_t ops_num = 1000;

void function_test(btree *bt, uint64_t ops);
int parse_input(int num, char **para);

int main(int argc, char *argv[]) {
    if(parse_input(argc, argv) != 0) {
        return 0;
    }

    btree *bt = new btree();

    bt->Initial(NODEPATH, NVM_NODE_SIZE, VALUEPATH, NVM_VALUE_SIZE);

    // bt->PrintInfo();
    if(test_type == 0) {
        ;
    } else if(test_type == 1) {
        function_test(bt, ops_num);
    }

    delete bt;
    return 0;
}

int parse_input(int num, char **para)
{
    if(num != 4) {
        cout << "input parameter nums incorrect! " << num << endl;
        return -1; 
    }

    using_existing_data = atoi(para[1]);
    test_type = atoi(para[2]);
    ops_num = atoi(para[3]);

    print_log(LV_INFO, "using_existing_data: %d(0:no, 1:yes)", using_existing_data);
    print_log(LV_INFO, "test_type:%d(0:Motivation test, 1:Function test)", test_type);
    print_log(LV_INFO, "ops_num:%llu", ops_num);
    return 0;
}

void function_test(btree *bt, uint64_t ops) {
    uint64_t i = 0;
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
        // memset(valuebuf, 0, sizeof(valuebuf));
        auto key = rnd_get.Next();
        snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
        string value(valuebuf, NVM_ValueSize);
        const string tmp_value = bt->btree_search(key);
        if(tmp_value.size() == 0) {
            printf("Error: Get key-value %lld faild.(key:%llx)\n", i, key);
        } else if(strncmp(value.c_str(), tmp_value.c_str(), NVM_ValueSize) != 0) {
            printf("Error: Get %llx key-value faild.(Expect:%s, but Get %s)\n", key, value.c_str(), tmp_value.c_str());
        }
    }
    printf("******Get test finished.*****\n");

    rocksdb::Random rnd_scan(0xdeadbeef);
    for(i = 0; i < ops / 100; i ++) {
        uint64_t key = rnd_scan.Next();
        std::vector<std::string>::iterator it;
        std::vector<std::string> values;
        int getcount = 10;
        bt->btree_search_range(key, MAX_KEY, values, getcount);
        int index = 0;
        for(it=values.begin(); it != values.end(); it++) 
        {
            printf("Info: Get range index %d is %s.\n", index, (*it).c_str());
            index ++;
        }
    }
    printf("******Get range test finished.******\n");

    rocksdb::Random rnd_delete(0xdeadbeef);
    for(uint64_t i = 0; i < ops; i ++) {
        auto key = rnd_delete.Next();
        bt->btree_delete(key);
    }
    printf("******Delete test finished.******\n");
    printf("******B+ tree function test finished.******\n");
    bt->printAll();
    bt->PrintInfo();
}