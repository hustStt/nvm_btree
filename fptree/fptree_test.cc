#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <future>

#include "fptree.h"
#include "../src/random.h"
#include "../src/debug.h"
#include "../src/statistic.h"
#include "../src/nvm_common.h"

#define LOGPATH "/mnt/pmem1/log_persistent"
#define PATH "/mnt/pmem1/ycsb"
#define NODEPATH   "/mnt/pmem1/persistent"

const uint64_t NVM_LOG_SIZE = 10 * (1ULL << 30);
const uint64_t NVM_NODE_SIZE = 20 * (1ULL << 30);


int using_existing_data = 0;
int test_type = 1;
int thread_num = 1;
uint64_t ops_num = 1000;

uint64_t start_time, end_time, use_time;

void motivationtest(FPTree* bt);
void nvm_print(int ops_num);

int main(int argc, char *argv[]) {

#ifdef NO_VALUE
    printf("Have define NO_VALUE\n");
#else 
    printf("Have not define NO_VALUE\n");
#endif

    if(AllocatorInit(LOGPATH, NVM_LOG_SIZE, NODEPATH, NVM_NODE_SIZE) < 0) {
        print_log(LV_ERR, "Initial allocator failed");
        return 0;
    }

    //btree *bt = new btree();
    FPTree* bt = new FPTree(15);
    

    // bt->PrintInfo();
    motivationtest(bt);

    
    //AllocatorExit();
    return 0;
}


// const uint64_t PutOps = 2000000;
// const uint64_t GetOps = 100000;
// const uint64_t DeleteOps = 100000;
// const uint64_t ScanOps = 1000;
// const uint64_t ScanCount = 100;

void motivationtest(FPTree* bt) {
    uint64_t i;
    uint64_t ops;
    Statistic stats;
    string value("value", NVM_ValueSize);
    printf("Value size is %d\n", value.size());
    //* 随机插入测试
    uint64_t rand_seed = 0xdeadbeef;
    vector<future<void>> futures(thread_num);

    //*插入初始化数据
    ops = 400000000;
    start_time = get_now_micros();
    for(int tid = 0; tid < thread_num; tid ++) {
        uint64_t from = (ops / thread_num) * tid;
        uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);
        auto f = async(launch::async, [&](int tid, uint64_t from, uint64_t to){
            rocksdb::Random64 rnd_put(rand_seed * (tid + 1));
            char valuebuf[NVM_ValueSize + 1];
            for(uint64_t i = from; i < to; i ++) {
                auto key = rnd_put.Next();
                snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
                string value(valuebuf, NVM_ValueSize);
                stats.start();
                // printf("Insert number %ld, key %llx.\n", i, key);

                char *pvalue = (char *)key;
                bt->insert(key, key);
                stats.end();
                stats.add_put();

                if ((i % 1000) == 0) {
                    cout<<"Put_test:"<<i;
                    stats.print_latency();
                    stats.clear_period();
                }
                // if ((i % 40000000) == 0) {
                //     printf("Number %ld", i / 40000000);
                //     bt->PrintStorage();
                // }
            }
            printf("thread %d finished.\n", tid);
        }, tid, from, to);

        futures.push_back(move(f));
    }
    for(auto &&f : futures) {
        if(f.valid()) {
            f.get();
        }
    }
    futures.clear();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Initial_insert test finished\n");
    nvm_print(ops);

    //* 随机写测试
    ops = 10000000;
    start_time = get_now_micros();
    for(int tid = 0; tid < thread_num; tid ++) {
        uint64_t from = (ops / thread_num) * tid;
        uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

        auto f = async(launch::async, [&bt, &rand_seed](int tid, uint64_t from, uint64_t to) {
            rocksdb::Random64 rnd_put(rand_seed * (tid + 1) * 2);
            char valuebuf[NVM_ValueSize + 1];
            for(uint64_t i = from; i < to; i ++) {
                auto key = rnd_put.Next();
                snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
                string value(valuebuf, NVM_ValueSize);
                // printf("Insert number %ld, key %llx.\n", i, key);
                char *pvalue = (char *)key;
                bt->insert(key, key);
            }
            print_log(LV_INFO, "thread %d finished.\n", tid);
        }, tid, from, to);

        futures.push_back(move(f));
    }
    for(auto &&f : futures) {
        if(f.valid()) {
            f.get();
        }
    }
    futures.clear();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Insert test finished\n");
    nvm_print(ops);

    //* 随机读测试
    ops = 10000000;
    start_time = get_now_micros();
    for(int tid = 0; tid < thread_num; tid ++) {
        uint64_t from = (ops / thread_num) * tid;
        uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

        auto f = async(launch::async, [&bt, &rand_seed](int tid, uint64_t from, uint64_t to) {
            rocksdb::Random64 rnd_get(rand_seed * (tid + 1));
            char valuebuf[NVM_ValueSize + 1];
            for(uint64_t i = from; i < to; i ++) {
                auto key = rnd_get.Next();
                uint64_t pvalue;
                pvalue = bt->find(key);
            }
            print_log(LV_INFO, "thread %d finished.\n", tid);
        }, tid, from, to);

        futures.push_back(move(f));
    }
    for(auto &&f : futures) {
        if(f.valid()) {
            f.get();
        }
    }
    futures.clear();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Get test finished\n");
    nvm_print(ops);

    //* Scan测试
    /*
    ops = 100;
    start_time = get_now_micros();
    int scantimes = 4;
    int scan_count = 100;
    while(scantimes > 0) {
    for(int tid = 0; tid < thread_num; tid ++) {
        uint64_t from = (ops / thread_num) * tid;
        uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

        auto f = async(launch::async, [&](int tid, uint64_t from, uint64_t to) {
            rocksdb::Random64 rnd_scan(rand_seed * (tid + 1));
            for(uint64_t i = from; i < to; i ++) {
                int size = scan_count;
                uint64_t key = rnd_scan.Next();

                void *pvalues[scan_count];
                

            }
            print_log(LV_INFO, "thread %d finished.\n", tid);
        }, tid, from, to);

        futures.push_back(move(f));
    }
    for(auto &&f : futures) {
        if(f.valid()) {
            f.get();
        }
    }
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Scan test finished , scan count %d.\n", scan_count);
    nvm_print(ops);

    scan_count *= 10;
    scantimes --;
    }
*/
    
    //* 删除测试
    ops = 10000000;
    start_time = get_now_micros();
    for(int tid = 0; tid < thread_num; tid ++) {
        uint64_t from = (ops / thread_num) * tid;
        uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

        auto f = async(launch::async, [&bt, &rand_seed](int tid, uint64_t from, uint64_t to) {
            rocksdb::Random64 rnd_delete(rand_seed * (tid + 1));
            char valuebuf[NVM_ValueSize + 1];
            for(uint64_t i = from; i < to; i ++) {
                auto key = rnd_delete.Next();
                bt->remove(key);
            }
            print_log(LV_INFO, "thread %d finished.\n", tid);
        }, tid, from, to);

        futures.push_back(move(f));
    }
    for(auto &&f : futures) {
        if(f.valid()) {
            f.get();
        }
    }
    futures.clear();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Delete test finished\n");
    nvm_print(ops);

    //D_RW(bt)->PrintStorage();
    //D_RW(bt)->PrintInfo();
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