#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include "tbb/tbb.h"

using namespace std;

#include "src/nvm_btree.h"
#include "src/nvm_nvtree.h"
#include "src/nvm_skiplist.h"

#define NODEPATH   "/mnt/pmem0/persistent"
#define VALUEPATH "/mnt/pmem0/value_persistent"

const uint64_t NVM_NODE_SIZE = 100 * (1ULL << 30);           // 100GB
const uint64_t NVM_VALUE_SIZE = 10 * (1ULL << 30);         // 10GB

#define WORKLOADSDIR "/root/sbh/RECIPE/index-microbench/workloads/"
// index types
enum {
    TYPE_FASTFAIR,
    TYPE_SKIPLIST,
    TYPE_NVTREE,
};

enum {
    OP_INSERT,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_D,
    WORKLOAD_E,
};

enum {
    RANDINT_KEY,
    STRING_KEY,
};

enum {
    UNIFORM,
    ZIPFIAN,
};

static uint64_t LOAD_SIZE = 64000000;
static uint64_t RUN_SIZE = 64000000;

// void ycsb_load_run_string(int index_type, int wl, int kt, int ap, int num_thread,
//         std::vector<Key *> &init_keys,
//         std::vector<Key *> &keys,
//         std::vector<int> &ranges,
//         std::vector<int> &ops)
// {
//     printf("No impl for string\n");
//     return ;
// }

void ycsb_load_run_randint(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = WORKLOADSDIR"loada_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = WORKLOADSDIR"loadb_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = WORKLOADSDIR"loadc_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = WORKLOADSDIR"loadd_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = WORKLOADSDIR"loade_unif_int.dat";
            txn_file = WORKLOADSDIR"txnse_unif_int.dat";
        }
    } else {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = WORKLOADSDIR"loada_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = WORKLOADSDIR"loadb_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = WORKLOADSDIR"loadc_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = WORKLOADSDIR"loadd_unif_int.dat";
            txn_file = WORKLOADSDIR"txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = WORKLOADSDIR"loade_unif_int.dat";
            txn_file = WORKLOADSDIR"txnse_unif_int.dat";
        }
    }

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");

    int count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(key);
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
        if(count % 1000 == 0) {
            printf("Read %d run keys\n", count);
        }
    }
    fprintf(stderr, "Read %d run keys\n", count);
    atomic<int> found(0);
    atomic<int> notfound(0);
    if (index_type == TYPE_FASTFAIR) {
        NVMBtree *bt = new NVMBtree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            //tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    char *pvalue = (char *)init_keys[i];
                    bt->Insert(init_keys[i], pvalue);
                    if(i % 1000 == 0) {
                        printf("Load %d keys\n", i);
                    }
                }
            //});
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }
        bt->Insert(0x54b130ba554f50e0UL, (char *)0x54b130ba554f50e0UL);
        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            //tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                std::cout<<"start: "<<scope.begin()<<std::endl;
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        char *pvalue = (char *)keys[i];
                        bt->Insert(keys[i], pvalue);
                    } else if (ops[i] == OP_READ) {
                        char *pvalue = nullptr;
                        bt->Get(keys[i], pvalue);
                        if ((uint64_t)pvalue != keys[i]) {
                            notfound ++;
                            std::cout << "[FAST-FAIR] wrong key read: " << (uint64_t)pvalue << " expected:" << keys[i] << std::endl;
                            break;
                            // exit(1);
                        } else {
                            found ++;
                        }
                    } else if (ops[i] == OP_SCAN) {
                        int resultsSize = ranges[i];
                        void *values[resultsSize];
                        bt->GetRange(keys[i], MAX_KEY, values, resultsSize);
                    } else if(ops[i] == OP_DELETE) {
                        bt->Delete(keys[i]);
                    }
                }
            //});
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
        printf("Found/Notfound: %d / %d\n", found.load(), notfound.load());
    }

}

int main(int argc, char **argv) {
    if (argc != 6) {
        std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads]\n";
        std::cout << "1. index type: fastfair nvtree skiplist\n";
        std::cout << "2. ycsb workload type: a, b, c, e\n";
        std::cout << "3. key distribution: randint, string\n";
        std::cout << "4. access pattern: uniform, zipfian\n";
        std::cout << "5. number of threads (integer)\n";
        return 1;
    }

    printf("%s, workload%s, %s, %s, threads %s\n", argv[1], argv[2], argv[3], argv[4], argv[5]);

    int index_type;
    if (strcmp(argv[1], "fastfair") == 0)
        index_type = TYPE_FASTFAIR;
    else if (strcmp(argv[1], "nvtree") == 0)
        index_type = TYPE_NVTREE;
    else if (strcmp(argv[1], "skiplist") == 0)
        index_type = TYPE_SKIPLIST;
    else {
        fprintf(stderr, "Unknown index type: %s\n", argv[1]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[2], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[2], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[2], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[2], "d") == 0) {
        wl = WORKLOAD_D;
    } else if (strcmp(argv[2], "e") == 0) {
        wl = WORKLOAD_E;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int kt;
    if (strcmp(argv[3], "randint") == 0) {
        kt = RANDINT_KEY;
    } else if (strcmp(argv[3], "string") == 0) {
        kt = STRING_KEY;
    } else {
        fprintf(stderr, "Unknown key type: %s\n", argv[3]);
        exit(1);
    }

    int ap;
    if (strcmp(argv[4], "uniform") == 0) {
        ap = UNIFORM;
    } else if (strcmp(argv[4], "zipfian") == 0) {
        ap = ZIPFIAN;
        fprintf(stderr, "Not supported access pattern: %s\n", argv[4]);
        exit(1);
    } else {
        fprintf(stderr, "Unknown access pattern: %s\n", argv[4]);
        exit(1);
    }
    if(AllocatorInit(NODEPATH, NVM_NODE_SIZE, VALUEPATH, NVM_VALUE_SIZE) < 0) {
        print_log(LV_ERR, "Initial allocator failed");
        return 0;
    }
    int num_thread = atoi(argv[5]);
    // tbb::task_scheduler_init init(tbb::task_scheduler_init::deferred);
    // if (num_thread >= 1) {
    //     init.initialize(num_thread);
    // }

    if (kt != STRING_KEY) {
        std::vector<uint64_t> init_keys;
        std::vector<uint64_t> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        ycsb_load_run_randint(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    } else {
        printf("No impl for ycsb string\n");
    //     std::vector<Key *> init_keys;
    //     std::vector<Key *> keys;
    //     std::vector<int> ranges;
    //     std::vector<int> ops;

    //     init_keys.reserve(LOAD_SIZE);
    //     keys.reserve(RUN_SIZE);
    //     ranges.reserve(RUN_SIZE);
    //     ops.reserve(RUN_SIZE);

    //     memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(Key *));
    //     memset(&keys[0], 0x00, RUN_SIZE * sizeof(Key *));
    //     memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
    //     memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

    //     ycsb_load_run_string(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    }
    // if (num_thread >= 1) {
    //     init.terminate();
    // }
    AllocatorExit();
    return 0;
}
