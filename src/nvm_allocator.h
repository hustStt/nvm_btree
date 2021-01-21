#pragma once

#include <cstring>
#include <string>
#include <libpmem.h>
#include <mutex>

#include <errno.h>
#include <err.h>


const uint64_t NVMSectorSize = 256;
const uint64_t MemReserved = (10 << 20);  // 保留 10M 空间
const uint64_t LogSize = (200 << 20);

class NVMAllocator {
public:
    NVMAllocator(const std::string path, size_t size, uint64_t log_num) {
        //pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem_));
        //映射NVM空间到文件
        pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_));
            
        if (pmemaddr_ == NULL) {
            printf("%s: map error, filepath %s, error: %s(%d)\n", __FUNCTION__, path.c_str(), strerror(errno), errno);
            exit(-1);
        } else {
            printf("%s: map at %p \n", __FUNCTION__, pmemaddr_);
        }
        begin_addr = pmemaddr_ + log_num / 8;
        capacity_ = size;
        log_num_ = log_num;
    }


    ~NVMAllocator() {
        pmem_unmap(pmemaddr_, mapped_len_);
    }

    char* getNewLog() {
        for (uint64_t i = 0; i < log_num_ / 8; i++) {
            if (pmemaddr_[i] == 0xFF) {
                continue;
            }
            for (int j = 0; j < 8;j ++) {
                if (((1 << j) & pmemaddr_[i]) == 0) {
                    return begin_addr + (8 * i + j) * LogSize;
                }
            }
        }
        return nullptr;
    }

    void deleteLog(char *addr) {
        pmem_memset_persist(addr, 0, LogSize);
        uint64_t i = (addr - begin_addr) / LogSize;
        pmemaddr_[i / 8] &= ~(1 << (i % 8)); 
    }

    void ResetZero() {
        pmem_memset_persist(pmemaddr_, 0, mapped_len_);
    }

private:
    char* pmemaddr_;
    char* begin_addr;
    size_t mapped_len_;
    uint64_t capacity_;
    uint64_t log_num_;
    int is_pmem_;
};


class LogAllocator {
private:
    char* pmemaddr_;
    uint64_t capacity_;
    uint64_t memused;
    std::mutex mut;
    char* cur_index_;
    NVMAllocator* nvm_alloc;
    
public:
    LogAllocator(NVMAllocator *log) {
        nvm_alloc = log;
        pmemaddr_ = nvm_alloc->getNewLog();
        capacity_ = LogSize;
    }

    ~LogAllocator() {
        nvm_alloc->deleteLog(pmemaddr_);
    }

    char* Allocate(size_t bytes, uint64_t aligns = 256) {
        std::lock_guard<std::mutex> lk(mut);
        uint64_t start_offset = (uint64_t)cur_index_;
        uint64_t allocated = (bytes + aligns - 1) & (~(aligns - 1));  //保证最小按照aligns分配
        start_offset = (start_offset + aligns - 1) & (~(aligns - 1)); // 按照aligns对齐
        memused = start_offset + allocated - (uint64_t)pmemaddr_;
        if(memused > capacity_) {
            printf("%s: log full\n", __FUNCTION__);
            return nullptr;
        }
        cur_index_ = (char *)(start_offset + allocated);
        return (char *)start_offset;

        // char *result = cur_index_;
        // uint64_t allocated = (bytes + NVMSectorSize - 1) & (~(NVMSectorSize - 1));  //保证最小按照NVMSectorSize分配
        // cur_index_ += allocated;
        // memused += allocated;
        // if(memused > capacity_) {
        //     printf("%s: NVM full\n", __FUNCTION__);
        //     return nullptr;
        // }
        // return result;
    }


    char* AllocateAligned(size_t bytes, size_t huge_page_size = 0) {
        mut.lock();
        char* result = cur_index_;
        cur_index_ += bytes;
        memused += bytes;
        mut.unlock();
        return result;
    }

    void PrintStorage(void) {
        printf("Storage capacity is %lluG %lluM %lluK %lluB\n", capacity_ >> 30, capacity_ >> 20 & (1024 - 1),
                        capacity_ >> 10 & (1024 - 1), capacity_ & (1024 - 1));
        printf("Storage used is %lluG %lluM %lluK %lluB\n", memused >> 30, memused >> 20 & (1024 - 1), 
                    memused >> 10 & (1024 - 1), memused & (1024 - 1));
    }

    bool StorageIsFull() {
        return memused + MemReserved >= capacity_;
    }

};


extern NVMAllocator *node_alloc;
extern NVMAllocator *value_alloc;
extern NVMAllocator *log_alloc;

int AllocatorInit(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize);
int AllocatorInit(const std::string &logpath, uint64_t logsize);
void AllocatorExit();

static LogAllocator * getNewLogAllocator() {
    if (log_alloc == nullptr) {
        return nullptr;
    }
    return new LogAllocator(log_alloc);
}

// static inline void clflush(char *data, int len)
// {
//     nvm_persist(data, len);
// }