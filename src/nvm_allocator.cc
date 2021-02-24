#include "nvm_common.h"
#include "nvm_allocator.h"

NVMAllocator *node_alloc = nullptr;
NVMAllocator *value_alloc = nullptr;
NVMLogPool *log_alloc_pool = nullptr;

atomic<uint64_t> perist_data(0);

int AllocatorInit(const std::string &logpath, uint64_t logsize, const std::string &allocator_path, 
                uint64_t allocator_size) {
    log_alloc_pool = new NVMLogPool(logpath, logsize);
    if(log_alloc_pool == nullptr) {
        delete log_alloc_pool;
        return -1;
    }
    node_alloc = new NVMAllocator(allocator_path, allocator_size);
    if(node_alloc == nullptr) {
        delete node_alloc;
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

    if(log_alloc_pool) {
        delete log_alloc_pool;
    }
}

void LogAllocator::writeKv(uint64_t off, int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(1, off, key, (uint64_t)value);
    nvm_memcpy_persist(logvalue, &tmp, 32);
}

void LogAllocator::updateKv(uint64_t off, int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(2, off, key, (uint64_t)value);
    nvm_memcpy_persist(logvalue, &tmp, 32);
}

void LogAllocator::deleteKey(uint64_t off, int64_t key) {
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(0, off, key, 0);
    nvm_memcpy_persist(logvalue, &tmp, 24);
}

void LogAllocator::operateTree(uint64_t src, uint64_t dst, int64_t key, int64_t type) {
    // 3分裂  子树内 
    // 4分裂  子树间   分裂后下刷前出问题恢复
    // 5合并  子树内 dram -- dram
    // 6合并  子树间 dram --> dram  合并后下刷前出问题恢复
    // 7合并  子树间 dram <-- dram
    // 8合并  子树间 dram <-- dram 完全合并
    // 9合并 子树间 nvm <-- dram 转换成update    dram <-- nvm 转换成insert
    // 10合并 子树间 dram --> nvm 转换成update    nvm --> dram 转换成insert
    char* logvalue = this->AllocateAligned(32);
    LogNode tmp(type, src, dst, key);
    nvm_memcpy_persist(logvalue, &tmp, 32);
}

void LogAllocator::log_persist(char* addr, SimpleLogNode& tmp, uint64_t aligns) {
    if ((uint64_t)addr + 24 - (uint64_t)last_index_ > aligns) {
        nvm_memcpy_persist(last_index_, &tmp, aligns);
        last_index_ = addr;
    } 
}

void LogAllocator::writeKv(int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(24);
    SimpleLogNode tmp(1, key, (uint64_t)value);
    log_persist(logvalue, tmp);
}

void LogAllocator::updateKv(int64_t key, char *value) {
    char* logvalue = this->AllocateAligned(24);
    SimpleLogNode tmp(2, key, (uint64_t)value);
    log_persist(logvalue, tmp);
}

void LogAllocator::deleteKey(int64_t key) {
    char* logvalue = this->AllocateAligned(24);
    SimpleLogNode tmp(0, key, 0);
    log_persist(logvalue, tmp);
}

void LogAllocator::operateTree(int64_t key, int64_t type) {
    // 3分裂  子树间
    // 4合并  子树间 dram <-- dram
    // 5合并  子树间 dram --> dram
    // 
    char* logvalue = this->AllocateAligned(24);
    SimpleLogNode tmp(type, key, 0);
    log_persist(logvalue, tmp);
}

static void alloc_memalign(void **ret, size_t alignment, size_t size) {
    // posix_memalign(ret, alignment, size);
    char *mem =  node_alloc->Allocate(size);
    *ret = mem;
}

void *LogAllocator::operator new(size_t size) {
    void *ret;
    alloc_memalign(&ret, 64, size);
    return ret;
}