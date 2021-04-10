/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-20 15:32:36
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <assert.h>
#include <string>
#include <stdint.h>

#include "nvm_btree.h"

using namespace std;
using namespace hbkv;

static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillrandom,"
    "readrandom,"
    "readseq,";

static const char* FLAGS_db_path = "/home/lzw/ceshi";  //暂时没用

//测试写的总kv数
static uint64_t FLAGS_nums = 1000000;

//测试读的数量，0代表等于FLAGS_nums；
static uint64_t FLAGS_reads = 100000;

//测试删除的数量，0代表等于FLAGS_nums；
static uint64_t FLAGS_deletes = 10000;

//测试修改的数量，0代表等于FLAGS_nums；
static uint64_t FLAGS_updates = 10000;

static uint64_t FLAGS_range_len = 1000;

// 测试线程个数，每个线程根据
static int FLAGS_threads = 1;

//value大小
static int FLAGS_value_size = 8;  //目录树fname长度，inode stat长度 

static int FLAGS_histogram = 1;   //0关闭，1开启 

//key 大小
static const int FLAGS_key_size = 8;   //不可修改

//Option
static uint64_t FLAGS_k_DIR_FIRST_HASH_MAX_CAPACITY = 0;
static uint64_t FLAGS_k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM = 0;   
static uint64_t FLAGS_k_DIR_SECOND_HASH_INIT_SIZE = 0;   
static double FLAGS_k_DIR_SECOND_HASH_TRIG_REHASH_TIMES = 0;  
static uint64_t FLAGS_k_INODE_MAX_ZONE_NUM = 0;   
static uint64_t FLAGS_k_INODE_HASHTABLE_INIT_SIZE = 0;  
static double FLAGS_k_INODE_HASHTABLE_TRIG_REHASH_TIMES = 0; 
static string FLAGS_k_node_allocator_path;
static uint64_t FLAGS_k_node_allocator_size = 0;   
static string FLAGS_k_file_allocator_path;
static uint64_t FLAGS_k_file_allocator_size = 0;   
static uint32_t FLAGS_k_thread_pool_count = 0;



namespace hbkv {

static inline uint64_t Random64(uint32_t *seed){
    return (((uint64_t)rand_r(seed)) << 32 ) | (uint64_t)rand_r(seed);
}

static inline uint64_t get_now_micros(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000000 + tv.tv_usec;
}

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

void trim_space(char *str) {
    char *p = str;
    char *p1;
    if(p)
    {
        p1 = p + strlen(str) - 1;
        while(*p && isspace(*p)) 
            p++;
        while(p1 > p && isspace(*p1)) 
            *p1--=0;
    }
    memmove(str, p, p1 - p + 1 + 1);
}

typedef enum {
    kNoneBenchmarkType = 0,
    kBenchmarkWriteType,
    kBenchmarkReadType,
    kBenchmarkDeleteType,
    kBenchmarkUpdateType,
    kBenchmarkRangeReadType,
    kBenchmarkTypeNums,   //该值代表type个数
} BenchmarkOperationType;

void BenchmarkOperationTypeToString(BenchmarkOperationType op_type, string &str){
    switch (op_type){
        case kNoneBenchmarkType:
            str = "not sure";
            break;
        case kBenchmarkWriteType:
            str = "write";
            break;
        case kBenchmarkReadType:
            str = "read";
            break;
        case kBenchmarkDeleteType:
            str = "delete";
            break;
        case kBenchmarkUpdateType:
            str = "update";
            break;
        case kBenchmarkRangeReadType:
            str = "range read";
            break;
        default:
            break;
    }
}

class Stats {
private:
    uint64_t start_;
    uint64_t finish_;
    double seconds_;
    uint64_t done_;
    uint64_t next_report_;
    uint64_t bytes_;
    uint64_t last_op_finish_;
    Histogram *histograms_[kBenchmarkTypeNums];
    string message_;

public:
    Stats() { Start(); }
    ~Stats() {
        for(uint32_t i = 0; i < kBenchmarkTypeNums; i++){
            if(histograms_[i] != nullptr) delete histograms_[i];
        }
    }

    void Start() {
        next_report_ = 100;
        done_ = 0;
        bytes_ = 0;
        seconds_ = 0;
        message_.clear();
        start_ = finish_ = last_op_finish_ = get_now_micros();
        for(uint32_t i = 0; i < kBenchmarkTypeNums; i++){
            histograms_[i] = nullptr;
        }
    }

    void Merge(Stats& other) {
        done_ += other.done_;
        bytes_ += other.bytes_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty()) message_ = other.message_;
        else {
            AppendWithSpace(&message_, other.message_);
        }
        if(FLAGS_histogram){
            for(uint32_t i = 0; i < kBenchmarkTypeNums; i++){
                if(histograms_[i] == nullptr){
                    histograms_[i] = other.histograms_[i];
                    other.histograms_[i] = nullptr;
                }
                else if(other.histograms_[i] != nullptr){
                    histograms_[i]->Merge((*(other.histograms_[i])));
                }
            }
        }
    }

    void Stop() {
        finish_ = get_now_micros();
        seconds_ = 1.0 * (finish_ - start_) * 1e-6;
    }

    void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

    void FinishedOp(uint64_t num_ops, BenchmarkOperationType op_type) {
        if (FLAGS_histogram) {
            uint64_t now = get_now_micros();
            uint64_t micros = now - last_op_finish_;
            if(histograms_[op_type] == nullptr){
                histograms_[op_type] = new Histogram();
            }
            histograms_[op_type]->Add(micros);
            // if (micros > 20000) {
            //     std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
            //     std::fflush(stderr);
            // }
            last_op_finish_ = now;
        }

        done_ += num_ops;
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            std::fprintf(stderr, "... finished %d ops%30s\r", done_, "");
            std::fflush(stderr);
        }
    }

    void AddBytes(int64_t n) { bytes_ += n; }

    void Report(const Slice& name) {
        if (done_ < 1) done_ = 1;

        std::string extra;
        double elapsed = (finish_ - start_) * 1e-6;
        if (bytes_ > 0) {
            char rate[100];
            std::snprintf(rate, sizeof(rate), "%6.4f MB/s",
                            (bytes_ / 1048576.0) / elapsed);
            extra = rate;
        }
        AppendWithSpace(&extra, message_);
        double iops = (elapsed != 0) ? 1.0 * done_ / elapsed : 0;
        std::fprintf(stdout, "%-12s : time:%.2f s; %8.3f micros/op; %6.0f ops/sec; %s%s\n",
                    name.ToString().c_str(), elapsed, elapsed * 1e6 / done_, iops, 
                    (extra.empty() ? "" : " "), extra.c_str());
        if (FLAGS_histogram) {
            for(int i = 0; i < kBenchmarkTypeNums; i++){
                if(histograms_[i] == nullptr) continue;
                string op_str;
                BenchmarkOperationTypeToString(static_cast<BenchmarkOperationType>(i), op_str);
                std::fprintf(stdout, "Microseconds per %s:\n%s\n", op_str.c_str(), 
                        histograms_[i]->ToString().c_str());
            }
        }
        std::fflush(stdout);
    }
};

struct SharedState {
    Mutex mu;
    CondVar cv;
    int total;
    int num_initialized;
    int num_done;
    bool start;

    SharedState(int num) : cv(&mu) { 
        total = num;
        num_initialized = 0;
        num_done = 0;
        start = false;
    }
    ~SharedState() {}
};

struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Stats stats;
  SharedState* shared;
  NVMBtree *db;

  ThreadState(int index)
      : tid(index) {
  }
  ~ThreadState() {}
};

struct ThreadArg {
    SharedState* shared;
    ThreadState* thread;
    void (*method)(ThreadState*);
    ThreadArg() {}
    ThreadArg(SharedState* s, ThreadState *th, void (*me)(ThreadState*)) : 
        shared(s), thread(th), method(me) {}
    ~ThreadArg() {}
};




void PrintEnvironment() {
#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stdout, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
        char line[1024];
        int num_cpus = 0;
        char cpu_type[1024];
        char cache_size[1024];
        char key[1024];
        char val[1024];
        while (fgets(line, sizeof(line), cpuinfo) != NULL) {
            const char* sep = strchr(line, ':');
            if (sep == NULL) {
            continue;
            }
            strncpy(key, line, sep - line);
            key[sep - line] = 0;
            strcpy(val, sep + 1);
            trim_space(key);
            trim_space(val);
            if (strcmp(key, "model name") == 0) {
            ++num_cpus;
            strcpy(cpu_type, val);
            } else if (strcmp(key, "cache size") == 0) {
                strcpy(cache_size, val);
            }
        }
        fclose(cpuinfo);
        fprintf(stdout, "CPU:        %d * %s\n", num_cpus, cpu_type);
        fprintf(stdout, "CPUCache:   %s\n", cache_size);
    }
#endif
}

void PrintHeader() {
    
    fprintf(stdout, "METADB:    \n");
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", FLAGS_key_size);
    fprintf(stdout, "Values:     %d bytes each\n", FLAGS_value_size);
    fprintf(stdout, "Threads:    %d \n", FLAGS_threads);
    fprintf(stdout, "Entries:    %lu (%.2f MB)\n", FLAGS_nums, (1.0 * (FLAGS_key_size + FLAGS_value_size) * FLAGS_nums) / 1048576.0);
    fprintf(stdout, "Reads:      %lu \n", (FLAGS_reads) ? FLAGS_reads : FLAGS_nums);
    fprintf(stdout, "Deletes:    %lu \n", (FLAGS_deletes) ? FLAGS_deletes : FLAGS_nums);
    fprintf(stdout, "Updates:    %lu \n", (FLAGS_updates) ? FLAGS_updates : FLAGS_nums);
    fprintf(stdout, "RangeLen:   %lu \n", (FLAGS_range_len) ? FLAGS_range_len : FLAGS_nums);
    fprintf(stdout, "------------------------------------------------\n");
    fflush(stdout);
}

void ThreadBody(void* v){
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    
    shared->mu.Lock();
    shared->num_initialized++;
    if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
    }
    while (!shared->start) {
        shared->cv.Wait();
    }
    shared->mu.Unlock();

    thread->stats.Start();
    (arg->method)(thread);
    thread->stats.Stop();

    shared->mu.Lock();
    shared->num_done++;
    if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
    }
    shared->mu.Unlock();
}

void RunBenchmark(NVMBtree *db, int n, char *name, void (*method)(ThreadState*)){
    SharedState *shared = new SharedState(n);

    ThreadArg *arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
        arg[i].shared = shared;
        arg[i].thread = new ThreadState(i);
        arg[i].thread->shared = shared;
        arg[i].thread->db = db;
        arg[i].method = method;
        ThreadPool::StartThread(&ThreadBody, &arg[i]);
    }

    shared->mu.Lock();
    while (shared->num_initialized < n) {
        shared->cv.Wait();
    }

    shared->start = true;
    shared->cv.SignalAll();

    while (shared->num_done < n) {
        shared->cv.Wait();
    }
    shared->mu.Unlock();

    for (int i = 1; i < n; i++) {
        arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
        delete arg[i].thread;
    }
    delete shared;
    delete[] arg;
}

void DoRandomWrite(ThreadState* thread){
    uint32_t seed = thread->tid + 1000;
    uint64_t nums = FLAGS_nums / FLAGS_threads;

    inode_id_t key;
    char *fname = new char[FLAGS_value_size + 1];
    inode_id_t value;
    uint64_t id = 0;
    uint64_t bytes = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % FLAGS_nums;
        key = id;
        value = id;
        snprintf(fname, FLAGS_value_size + 1, "%0*llu", FLAGS_value_size, id);

        //ret = thread->db->DirPut(key, Slice(fname, FLAGS_value_size), value);

        if(ret != 0){
            fprintf(stderr, "dir put error! key:%lu fname:%.*s value:%lu\n", key, FLAGS_value_size, fname, value);
            fflush(stderr);
            exit(1);
        }
        bytes += (FLAGS_key_size + FLAGS_value_size + FLAGS_key_size);
        thread->stats.FinishedOp(1, kBenchmarkWriteType);
    }
    delete fname;
    thread->stats.AddBytes(bytes);
}

void ReadSeq(ThreadState* thread){

}

void ReadRandom(ThreadState* thread){
    uint32_t seed = thread->tid + 1000;
    uint64_t nums = (FLAGS_reads == 0) ? FLAGS_nums / FLAGS_threads : FLAGS_reads / FLAGS_threads;

    uint64_t key;
    string fname;
    uint64_t value;
    uint64_t id = 0;
    uint64_t found = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % FLAGS_nums;
        key = id;
        value = 0;
        //snprintf(fname, FLAGS_value_size + 1, "%0*llu", FLAGS_value_size, id);

        //ret = thread->db->DirGet(key, Slice(fname, FLAGS_value_size), value);
        ret = thread->db->Get(key,fname);
        if(ret == 0) {
            //if(value != key) fprintf(stdout, "dir get wrong value! key:%lu fname:%.*s value:%lu\n", key, FLAGS_value_size, fname, value);
            found++;
        }
        else{
            //fprintf(stdout, "dir read no found! key:%lu fname:%.*s\n", key, FLAGS_value_size, fname);
        }
        thread->stats.FinishedOp(1, kBenchmarkReadType);
    }

    delete fname;

    char msg[100];
    snprintf(msg, sizeof(msg), "(%lu of %lu found)", found, nums);
    thread->stats.AddMessage(msg);
}

  void DoWrite(ThreadState* thread, bool seq) {
    uint32_t seed = thread->tid + 1000;
    uint64_t nums = FLAGS_nums / FLAGS_threads;

    uint64_t key;
    char *fname = new char[FLAGS_value_size + 1];
    uint64_t value;
    uint64_t id = 0;
    uint64_t bytes = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = seq ? i : (Random64(&seed) % FLAGS_nums);
        key = id;
        value = id;
        snprintf(fname, FLAGS_value_size + 1, "%0*llu", FLAGS_value_size, id);

        //ret = thread->db->DirPut(key, Slice(fname, FLAGS_value_size), value);
        thread->db->Insert(key, fname);
        bytes += (FLAGS_key_size + FLAGS_value_size + FLAGS_key_size);
        thread->stats.FinishedOp(1, kBenchmarkWriteType);
    }
    delete fname;
    thread->stats.AddBytes(bytes);
  }
  
  void WriteSeq(ThreadState* thread) { DoWrite(thread, true); }

  void WriteRandom(ThreadState* thread) { DoWrite(thread, false); }

  


void PrintStats(NVMBtree *db) {
    
}

void RunTest(){
    NVMBtree *db;
    
    SetOption(option);

    PrintHeader();


    const char* benchmarks = FLAGS_benchmarks;
    char name[100];
    memset(name, 0, 100);
    while(benchmarks != nullptr){
        const char* sep = strchr(benchmarks, ',');
        if (sep == nullptr) {
            strcpy(name, benchmarks);
            benchmarks = nullptr;
        } else {
            strncpy(name, benchmarks, sep - benchmarks);
            name[sep - benchmarks] = '\0';
            benchmarks = sep + 1;
        }
        void (*method)(ThreadState*) = nullptr;
        bool fresh_db = false;

        if(strcmp(name, "fillrandom") == 0){
            method = WriteRandom;
        }
        else if (strcmp(name, "readrandom") == 0){
            method = ReadRandom;
        } else if (strcmp(name, "fillseq") == 0){
            method = WriteSeq;
        } else if (strcmp(name, "readseq") == 0){
            method = ReadSeq;
        }
        else if (strcmp(name, "stats") == 0){
            PrintStats(db);
        }
        else {
            if(strlen(name) > 0){
                fprintf(stderr, "unknown benchmark '%s'\n", name);
            }
        }

        if(fresh_db){

        }

        if (method != NULL) {
            RunBenchmark(db, FLAGS_threads, name, method);
        }
    }

    delete db;
}

} // namespace name

int main(int argc, char** argv){
    int i;
    char benchmarks[200];
    char buff[100];
    for (i = 1; i < argc; i++) {
        double d;
        int n;
        uint64_t nums;
        char junk;

        if(sscanf(argv[i], "--benchmarks=%200s%c", (char *)&benchmarks, &junk) == 1) {
            FLAGS_benchmarks = benchmarks;
        } else if (sscanf(argv[i], "--db=%100s%c", (char *)&buff, &junk) == 1) {
            FLAGS_db_path = buff;
        } else if (sscanf(argv[i], "--nums=%llu%c", &nums, &junk) == 1) {
            FLAGS_nums = nums;
        } else if (sscanf(argv[i], "--reads=%llu%c", &nums, &junk) == 1) {
            FLAGS_reads = nums;
        } else if (sscanf(argv[i], "--deletes=%llu%c", &nums, &junk) == 1) {
            FLAGS_deletes = nums;
        } else if (sscanf(argv[i], "--updates=%llu%c", &nums, &junk) == 1) {
            FLAGS_updates = nums;
        } else if (sscanf(argv[i], "--range_len=%llu%c", &nums, &junk) == 1) {
            FLAGS_range_len = nums;
        } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1) {
            FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--k_DIR_FIRST_HASH_MAX_CAPACITY=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_DIR_FIRST_HASH_MAX_CAPACITY = nums;
        } else if (sscanf(argv[i], "--k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM = nums;
        } else if (sscanf(argv[i], "--k_DIR_SECOND_HASH_INIT_SIZE=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_DIR_SECOND_HASH_INIT_SIZE = nums;
        } else if (sscanf(argv[i], "--k_DIR_SECOND_HASH_TRIG_REHASH_TIMES=%lf%c", &d, &junk) == 1) {
            FLAGS_k_DIR_SECOND_HASH_TRIG_REHASH_TIMES = d;
        } else if (sscanf(argv[i], "--k_INODE_MAX_ZONE_NUM=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_INODE_MAX_ZONE_NUM = nums;
        } else if (sscanf(argv[i], "--k_INODE_HASHTABLE_INIT_SIZE=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_INODE_HASHTABLE_INIT_SIZE = nums;
        } else if (sscanf(argv[i], "--k_INODE_HASHTABLE_TRIG_REHASH_TIMES=%lf%c", &d, &junk) == 1) {
            FLAGS_k_INODE_HASHTABLE_TRIG_REHASH_TIMES = d;
        } else if (sscanf(argv[i], "--k_node_allocator_path=%100s%c", (char *)&buff, &junk) == 1) {
            FLAGS_k_node_allocator_path.assign(buff, strlen(buff));
        } else if (sscanf(argv[i], "--k_node_allocator_size=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_node_allocator_size = nums;
        } else if (sscanf(argv[i], "--k_file_allocator_path=%100s%c", (char *)&buff, &junk) == 1) {
            FLAGS_k_file_allocator_path.assign(buff, strlen(buff));
        } else if (sscanf(argv[i], "--k_file_allocator_size=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_file_allocator_size = nums;
        } else if (sscanf(argv[i], "--k_thread_pool_count=%llu%c", &nums, &junk) == 1) {
            FLAGS_k_thread_pool_count = nums;
        } else {
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
        }

    }    
    hbkv::RunTest();
    return 0;
}