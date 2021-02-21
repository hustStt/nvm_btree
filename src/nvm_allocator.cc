#include "nvm_common.h"
#include "nvm_allocator.h"

NVMAllocator *node_alloc = nullptr;
NVMAllocator *value_alloc = nullptr;
NVMAllocator *log_alloc = nullptr;

atomic<uint64_t> perist_data(0);

int AllocatorInit(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize) {
    node_alloc = new NVMAllocator(path, keysize);
    if(node_alloc == nullptr) {
        return -1;
    }
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete node_alloc;
        return -1;
    }
    // perist_data = 0;
    return 0;
}

int AllocatorInit(const std::string &logpath, uint64_t logsize) {
    log_alloc = new NVMAllocator(logpath, logsize);
    if(log_alloc == nullptr) {
        delete log_alloc;
        return -1;
    }
    // perist_data = 0;
    return 0;
}

void AllocatorExit() {
    if(node_alloc) {
        delete node_alloc;
    } 

    if(value_alloc) {
        delete value_alloc;
    }

    if(log_alloc) {
        delete log_alloc;
    }
}

void LogAllocator::writeKv(uint64_t off, int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(26);
    char* tmp = logvalue;
    uint16_t type = 1;
    memcpy(logvalue, &type, 2);
    logvalue += 2;
    memcpy(logvalue, &off, 8);
    logvalue += 8;
    memcpy(logvalue, &key, 8);
    logvalue += 8;
    memcpy(logvalue, &value, 8);
    nvm_persist(tmp, 26);
}

void LogAllocator::updateKv(uint64_t off, int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(26);
    char* tmp = logvalue;
    uint16_t type = 2;
    memcpy(logvalue, &type, 2);
    logvalue += 2;
    memcpy(logvalue, &off, 8);
    logvalue += 8;
    memcpy(logvalue, &key, 8);
    logvalue += 8;
    memcpy(logvalue, &value, 8);
    nvm_persist(tmp, 26);
}

void LogAllocator::deleteKey(uint64_t off, int64_t key) {
    char *logvalue = this->AllocateAligned(18);
    char* tmp = logvalue;
    uint16_t type = 0;
    memcpy(logvalue, &type, 2);
    logvalue += 2;
    memcpy(logvalue, &off, 8);
    logvalue += 8;
    memcpy(logvalue, &key, 8);
    nvm_persist(tmp, 18);
}

void LogAllocator::operateTree(uint64_t src, uint64_t dst, int64_t key, uint16_t type) {
    char *logvalue = this->AllocateAligned(26);
    char* tmp = logvalue;
    // 3分裂  子树内 
    // 4分裂  子树间   分裂后下刷前出问题恢复
    // 5合并  子树内 dram --> dram
    // 6合并  子树内 dram <-- dram
    // 7合并  子树间 dram --> dram  合并后下刷前出问题恢复
    // 8合并  子树间 dram <-- dram
    // 9合并  子树间 dram <-- dram 完全合并
    // 10合并 子树间 nvm <-- dram 转换成update    dram <-- nvm 转换成insert
    // 11合并 子树间 dram --> nvm 转换成update    nvm --> dram 转换成insert
    memcpy(logvalue, &type, 2);
    logvalue += 2;
    memcpy(logvalue, &src, 8);
    logvalue += 8;
    memcpy(logvalue, &dst, 8);
    logvalue += 8;
    memcpy(logvalue, &key, 8);
    nvm_persist(tmp, 26);
}