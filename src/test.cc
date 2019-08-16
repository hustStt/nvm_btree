#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_btree.h"
#include "random.h"
#include "debug.h"
#include "statistic.h"

#define NODEPATH   "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"


const uint64_t NVM_NODE_SIZE = 45 * (1ULL << 30);           // 45GB
const uint64_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB
const uint64_t MAX_KEY = ~(0ULL);

int using_existing_data = 0;
int test_type = 1;
uint64_t ops_num = 1000;

uint64_t start_time, end_time, use_time;

void function_test(NVMBtree *bt, uint64_t ops);
void motivationtest(NVMBtree *bt);
void nvm_print(int ops_num);
int parse_input(int num, char **para);

int main(int argc, char *argv[]) {
    if(parse_input(argc, argv) != 0) {
        return 0;
    }

    if(AllocatorInit(NODEPATH, NVM_NODE_SIZE, VALUEPATH, NVM_VALUE_SIZE) < 0) {
        print_log(LV_ERR, "Initial allocator failed");
        return 0;
    }


    NVMBtree *bt = new NVMBtree();

    // bt->PrintInfo();
    if(test_type == 0) {
        motivationtest(bt);
    } else if(test_type == 1) {
        function_test(bt, ops_num);
    }

    delete bt;
    AllocatorExit();
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

void function_test(NVMBtree *bt, uint64_t ops) {
    uint64_t i = 0;
    char valuebuf[NVM_ValueSize + 1];
    printf("******B+ tree function test start.******\n");
    rocksdb::Random rnd_put(0xdeadbeef); 
    for(uint64_t i = 0; i < ops; i ++) {
        auto key = rnd_put.Next();
        snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
        string value(valuebuf, NVM_ValueSize);
        bt->Insert(key, value);
    } 
    printf("******Insert test finished.******\n");

    rocksdb::Random rnd_get(0xdeadbeef);
    for(uint64_t i = 0; i < ops; i ++) {
        // memset(valuebuf, 0, sizeof(valuebuf));
        auto key = rnd_get.Next();
        snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
        string value(valuebuf, NVM_ValueSize);
        const string tmp_value = bt->Get(key);
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
        bt->GetRange(key, MAX_KEY, values, getcount);
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
    // bt->printAll();
    bt->PrintInfo();
}

const uint64_t PutOps = 2000000;
const uint64_t GetOps = 100000;
const uint64_t DeleteOps = 100000;
const uint64_t ScanOps = 1000;
const uint64_t ScanCount = 100;

void motivationtest(NVMBtree *bt) {
    uint64_t i;
    Statistic stats;
    string value("value", NVM_ValueSize);
    printf("Value size is %d\n", value.size());
    //* 随机插入测试
    rocksdb::Random rnd_insert(0xdeadbeef);
    start_time = get_now_micros();
    for (i = 1; i <= PutOps; i++) {
        auto key = rnd_insert.Next() & ((1ULL << 40) - 1);
        stats.start();
        bt->Insert(key, value);
        stats.end();
        stats.add_put();

        if ((i % 1000) == 0) {
            cout<<"Put_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }

        if(bt->StorageIsFull()) {
            break;
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Insert test finished\n");
    nvm_print(i-1);

    //* 随机读测试
    rocksdb::Random rnd_get(0xdeadbeef); 
    start_time = get_now_micros();
    for (i = 1; i <= GetOps; i++) {
        auto key = rnd_get.Next() & ((1ULL << 40) - 1);
        stats.start();
        const string value = bt->Get(key);
        stats.end();
        stats.add_get();

        if ((i % 1000) == 0) {
            cout<<"Get_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Get test finished\n");
    nvm_print(i-1);

    //* Scan测试
    rocksdb::Random rnd_scan(0xdeadbeef); 
    start_time = get_now_micros();
    for (i = 1; i <= ScanOps; i++) {
        auto key = rnd_scan.Next() & ((1ULL << 40) - 1);
        int size = ScanCount;
        std::vector<std::string> values;
        stats.start();
        bt->GetRange(key, MAX_KEY, values, size);
        stats.end();
        stats.add_scan();

        if ((i % 100) == 0) {
            cout<<"Scan_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Scan test finished\n");
    nvm_print(i-1);

    //* 删除测试
    rocksdb::Random rnd_delete(0xdeadbeef);
    start_time = get_now_micros();
    for (i = 1; i <= DeleteOps; i++) {

        auto key = rnd_delete.Next() & ((1ULL << 40) - 1);
        stats.start();
        bt->btree_delete(key);
        stats.end();
        stats.add_delete();

        if ((i % 1000) == 0) {
            cout<<"Delete_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Delete test finished\n");
    nvm_print(i-1);
    bt->PrintStorage();
    bt->PrintInfo();
    print_log(LV_INFO, "end!");
}


void nvm_print(int ops_num)
{   
    printf("-------------   write to nvm  start: ----------------------\n");
    printf("key: %uB, value: %uB, number: %llu\n", NVM_KeySize, NVM_ValueSize, ops_num);
    printf("time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", 1.0 * use_time * 1e-6, 
                1.0 * (NVM_KeySize + NVM_ValueSize) * ops_num * 1e6 / use_time / 1048576, 
                1.0 * ops_num * 1e6 / use_time);
    printf("-------------   write to nvm  end: ----------------------\n");
}