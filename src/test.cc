#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_btree.h"
#include "random.h"
#include "debug.h"
#include "statistic.h"
#include "single_pmdk.h"

#define NODEPATH   "/mnt/pmem0/persistent"
#define VALUEPATH "/mnt/pmem0/value_persistent"
#define LOGPATH "/mnt/pmem0/log_persistent"

const uint64_t NVM_NODE_SIZE = 100 * (1ULL << 30);           // 45GB
const uint64_t NVM_VALUE_SIZE = 10 * (1ULL << 30);         // 10GB
const uint64_t NVM_LOG_SIZE = 10 * (1ULL << 30);

int using_existing_data = 0;
int test_type = 1;
int thread_num = 1;
uint64_t ops_num = 1000;

uint64_t start_time, end_time, use_time;

void function_test(NVMBtree *bt, uint64_t ops);
void motivationtest(NVMBtree *bt, uint64_t load_num, TOID(nvmbtree) nvmbt);
void nvm_print(int ops_num);
int parse_input(int num, char **para);

static inline int file_exists(char const *file) { return access(file, F_OK); }


int main(int argc, char *argv[]) {
    if(parse_input(argc, argv) != 0) {
        return 0;
    }

    if(AllocatorInit(LOGPATH, NVM_LOG_SIZE) < 0) {
        print_log(LV_ERR, "Initial allocator failed");
        return 0;
    }

#ifdef NO_VALUE
    printf("Have define NO_VALUE\n");
#else 
    printf("Have not define NO_VALUE\n");
#endif

    char* persistent_path = "/mnt/pmem0/mytest";

    TOID(nvmbtree) nvmbt = TOID_NULL(nvmbtree);
    PMEMobjpool *pop;

    if (file_exists(persistent_path) != 0) {
        pop = pmemobj_create(persistent_path, "btree", 30000000000,
                            0666); // make 1GB memory pool
        nvmbt = POBJ_ROOT(pop, nvmbtree);
        D_RW(nvmbt)->constructor(pop);
    } else {
        pop = pmemobj_open(persistent_path, "btree");
        nvmbt = POBJ_ROOT(pop, nvmbtree);
        D_RW(nvmbt)->setPMEMobjpool(pop);
    }

    NVMBtree *bt = new NVMBtree(pop);

    // bt->PrintInfo();
    if(test_type == 0) {
        motivationtest(bt, ops_num, nvmbt);
    } else if(test_type == 1) {
        function_test(bt, ops_num);
    }

    delete bt;
    AllocatorExit();
    return 0;
}

int parse_input(int num, char **para)
{
    if(num < 4) {
        cout << "input parameter nums incorrect! " << num << endl;
        return -1; 
    }

    using_existing_data = atoi(para[1]);
    test_type = atoi(para[2]);
    ops_num = atoi(para[3]);

    if(num > 4) {
        thread_num = atoi(para[4]);
    }

    print_log(LV_INFO, "using_existing_data: %d(0:no, 1:yes)", using_existing_data);
    print_log(LV_INFO, "test_type: %d(0:Motivation test, 1:Function test)", test_type);
    print_log(LV_INFO, "ops_num: %llu", ops_num);
    print_log(LV_INFO, "thread number: %d", thread_num);
    return 0;
}

void function_test(NVMBtree *bt, uint64_t ops_param) {
    uint64_t i = 0;
    char valuebuf[NVM_ValueSize + 1];
    rocksdb::Random64 rnd_put(0xdeadbeef);
    rocksdb::Random64 rnd_get(0xdeadbeef);
    rocksdb::Random64 rnd_scan(0xdeadbeef);
    rocksdb::Random64 rnd_delete(0xdeadbeef);
    rocksdb::Random64 rnd_delcheck(0xdeadbeef);
    rocksdb::Random64 rnd_delall(0xdeadbeef);
    printf("******B+ tree function test start.******\n");
    uint64_t ops= 1000;
    if(ops > ops_param) {
        ops = ops_param;
    }
    int loop = 1;
    uint64_t rand_seed = 0xdeadbeef;
    vector<future<void>> futures(thread_num);

    while(ops <= ops_param){
        rand_seed /= loop;
        for(int tid = 0; tid < thread_num; tid ++) {
            uint64_t from = (ops / thread_num) * tid;
            uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

            auto f = async(launch::async, [&bt, &rand_seed, &ops](int tid, uint64_t from, uint64_t to) {
                rocksdb::Random64 rnd_put(rand_seed * (tid + 1));
                char valuebuf[NVM_ValueSize + 1];
                for(uint64_t i = from; i < to; i ++) {
                    auto key = rnd_put.Next();
                    snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
                    string value(valuebuf, NVM_ValueSize);
                    // printf("Insert number %ld, key %llx.\n", i, key);
                    bt->Insert(key, value);
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
        printf("******Insert test finished.******\n");
        // bt->Print();

        for(int tid = 0; tid < thread_num; tid ++) {
            uint64_t from = (ops / thread_num) * tid;
            uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

            auto f = async(launch::async, [&bt, &rand_seed, &ops](int tid, uint64_t from, uint64_t to) {
                rocksdb::Random64 rnd_get(rand_seed * (tid + 1));
                char valuebuf[NVM_ValueSize + 1];
                for(uint64_t i = from; i < to; i ++) {
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
            }, tid, from, to);
            futures.push_back(move(f));
        }
        for(auto &&f : futures) {
            if(f.valid()) {
                f.get();
            }
        }
        futures.clear();
        printf("******Get test finished.*****\n");

        for(int tid = 0; tid < thread_num; tid ++) {
            uint64_t from = (ops / thread_num) * tid;
            uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

            auto f = async(launch::async, [&bt, &rand_seed, &ops](int tid, uint64_t from, uint64_t to) {
                rocksdb::Random64 rnd_scan(rand_seed * (tid + 1));
                char valuebuf[NVM_ValueSize + 1];
                for(uint64_t i = from; i < to; i ++) {
                    auto key = rnd_scan.Next();
                    if(i % 100 == 0) {
                        std::vector<std::string> values;
                        int getcount = 10;
                        bt->GetRange(key, MAX_KEY, values, getcount);
                        int index = 0;
                        std::vector<std::string>::iterator it;
                        printf("Get rang no. %lld, key is %llx.\n", i, key);
                        for(it=values.begin(); it != values.end(); it++) 
                        {
                            printf("Info: Get range index %d is %s.\n", index, (*it).c_str());
                            index ++;
                        }
                    }
                } 
            }, tid, from, to);
            futures.push_back(move(f));
        }
        for(auto &&f : futures) {
            if(f.valid()) {
                f.get();
            }
        }
        futures.clear();
        printf("******Get range test finished.******\n");

         for(int tid = 0; tid < thread_num; tid ++) {
            uint64_t from = (ops / thread_num) * tid;
            uint64_t to = (tid == thread_num - 1) ? ops : from + (ops / thread_num);

            auto f = async(launch::async, [&bt, &rand_seed, &ops](int tid, uint64_t from, uint64_t to) {
                rocksdb::Random64 rnd_delete(rand_seed * (tid + 1));
                char valuebuf[NVM_ValueSize + 1];
                for(uint64_t i = from; i < to; i ++) {
                    uint64_t key = rnd_delete.Next();
                    if(i % 5 == 0) {
                        bt->Delete(key);
                    }
                }
                rocksdb::Random64 rnd_delcheck(rand_seed * (tid + 1));
                for(uint64_t i = from; i < to; i ++) {
                    uint64_t key = rnd_delcheck.Next();
                    snprintf(valuebuf, sizeof(valuebuf), "%020llu", i * i);
                    string value(valuebuf, NVM_ValueSize);
                    string tmp_value = bt->Get(key);
                    if(tmp_value.size() == 0) {
                        if(i % 5) {
                            printf("Error: Get no. %lld (key:%llx) key-value should deleted.\n", i, key);
                        }
                    } else if(strncmp(value.c_str(), tmp_value.c_str(), NVM_ValueSize) != 0) {
                        printf("Error: Get no. %lld key %llx key-value faild.(Expect:%s, but Get %s)\n", i, key, value.c_str(), tmp_value.c_str());
                    }
                }
            }, tid, from, to);

            futures.push_back(move(f));
        }
        for(auto &&f : futures) {
            if(f.valid()) {
                f.get();
            }
        }
        futures.clear();
        printf("******Delete test finished.******\n");
        printf("******Test one loop...\n");
        ops *= 10;
        loop ++;
    }
    // ops= 1000;
    // if(ops > ops_param) {
    //     ops = ops_param;
    // }
    // while(ops <= ops_param) {
    //     for(i = 0; i < ops; i ++) {
    //         uint64_t key = rnd_delall.Next();
    //         bt->Delete(key);
    //     }
    //     ops *= 10;
    // } 
    printf("******Delete test finished.******\n");
    printf("******B+ tree function test finished.******\n");
    // bt->printAll();
    // bt->Print();
    bt->PrintInfo();
}

// const uint64_t PutOps = 2000000;
// const uint64_t GetOps = 100000;
// const uint64_t DeleteOps = 100000;
// const uint64_t ScanOps = 1000;
// const uint64_t ScanCount = 100;

void motivationtest(NVMBtree *bt, uint64_t load_num, TOID(nvmbtree) nvmbt) {
    uint64_t i;
    uint64_t ops;
    Statistic stats;
    string value("value", NVM_ValueSize);
    printf("Value size is %d\n", value.size());
    //* 随机插入测试
    uint64_t rand_seed = 0xdeadbeef;
    vector<future<void>> futures(thread_num);

    //*插入初始化数据
    ops = load_num;
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
#ifdef NO_VALUE
                char *pvalue = (char *)key;
                bt->Insert(key, pvalue);
#else
                bt->Insert(key, value);
#endif
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
    uint64_t start_time_s = get_now_micros();
    //bt->test(nvmbt);
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("sync time: %f s\n", (end_time - start_time_s) * 1e-6);
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
#ifdef NO_VALUE
                char *pvalue = (char *)key;
                bt->Insert(key, pvalue);
#else
                bt->Insert(key, value);
#endif
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
    bt->PrintInfo();

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
#ifdef NO_VALUE
                char *pvalue = nullptr;
                bt->Get(key, pvalue);
#else
                const string value = bt->Get(key);
#endif
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
#ifdef NO_VALUE
                void *pvalues[scan_count];
                bt->GetRange(key, MAX_KEY, pvalues, size);
#else
                std::vector<std::string> values;
                bt->GetRange(key, MAX_KEY, values, size);
#endif
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
                bt->Delete(key);;
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

    bt->PrintStorage();
    bt->PrintInfo();
    print_log(LV_INFO, "end!");
}

void single_thread_motivationtest(NVMBtree *bt) {
    uint64_t i;
    Statistic stats;
    string value("value", NVM_ValueSize);
    printf("Value size is %d\n", value.size());
    //* 随机插入测试
    rocksdb::Random64 rnd_insert(0xdeadbeef);
    start_time = get_now_micros();
    for (i = 1; i <= PutOps; i++) {
        auto key = rnd_insert.Next() ;
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

    start_time = get_now_micros();
    for (i = 1; i <= GetOps; i++) {
        auto key = rnd_insert.Next() ;
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
    printf("Last_Insert test finished\n");
    nvm_print(i-1);

    //* 随机读测试
    rocksdb::Random64 rnd_get(0xdeadbeef); 
    start_time = get_now_micros();
    for (i = 1; i <= GetOps; i++) {
        auto key = rnd_get.Next() ;
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
    rocksdb::Random64 rnd_scan(0xdeadbeef); 
    int scan_count = 100;
    for(int j = 1; j < 5; j++) {
        start_time = get_now_micros();
        for (i = 1; i <= 100; i++) {
            uint64_t key = rnd_scan.Next() ;
            key = key >> j;
            int size = scan_count;
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
        printf("Scan test finished , scan count %d.\n", scan_count);
        nvm_print(i-1);
        scan_count *= 10;
    }
    //* 删除测试
    rocksdb::Random64 rnd_delete(0xdeadbeef);
    start_time = get_now_micros();
    for (i = 1; i <= DeleteOps; i++) {

        auto key = rnd_delete.Next() ;
        stats.start();
        bt->Delete(key);
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