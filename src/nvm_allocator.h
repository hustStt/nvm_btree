#pragma once

#include <cstring>
#include <string>
#include <libpmem.h>
#include <mutex>


const uint64_t NVMSectorSize = 256;
const uint64_t MemReserved = (10 << 20);  // 保留 10M 空间

class NVMAllocator {
public:
    NVMAllocator(const std::string path, size_t size) {
        //pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem_));
        //映射NVM空间到文件
        pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_));
            
        if (pmemaddr_ == NULL) {
            printf("%s: map error, filepath %s\n", __FUNCTION__, path.c_str());
            exit(-1);
        } else {
            printf("%s: map at %p \n", __FUNCTION__, pmemaddr_);
        }
        capacity_ = size;
        memused = 0;
        cur_index_ = pmemaddr_;
    }


    ~NVMAllocator() {
        pmem_unmap(pmemaddr_, mapped_len_);
    }

    char* Allocate(size_t bytes) {
        mut.lock();
        char *result = cur_index_;
        uint64_t allocated = (bytes + NVMSectorSize - 1) & (~(NVMSectorSize - 1));  //保证最小按照NVMSectorSize分配
        cur_index_ += allocated;
        memused += allocated;
        if(memused > capacity_) {
            printf("%s: NVM full\n", __FUNCTION__);
            result = nullptr;
        }
        mut.unlock();
        return result;
    }

    char* AllocateAligned(size_t bytes, size_t huge_page_size = 0) {
        mut.lock();
        char* result = cur_index_;
        cur_index_ += bytes;
        memused += bytes;
        mut.unlock();
        return result;
    }

    void ResetZero() {
        pmem_memset_persist(pmemaddr_, 0, mapped_len_);
    }

    void PrintStorage(void) {
        printf("Storage capacity is %lluG %lluM %lluK %lluB\n", capacity_ >> 30, capacity_ >> 20 & (1024 - 1),
                        capacity_ >> 10 & (1024 - 1), capacity_ & (1024 - 1));
        printf("Storage used is %lluG %lluM %lluK %lluB\n", memused >> 30, memused >> 20 & (1024 - 1), 
                    memused >> 10 & (1024 - 1), memused & (1024 - 1));
    }

    size_t BlockSize() {
        return 0;
    }

    bool StorageIsFull() {
        return memused + MemReserved >= capacity_;
    }

private:
    char* pmemaddr_;
    size_t mapped_len_;
    uint64_t capacity_;
    uint64_t memused;
    int is_pmem_;
    std::mutex mut;
    char* cur_index_;
};

extern NVMAllocator *node_alloc;
extern NVMAllocator *value_alloc;

int AllocatorInit(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize);
void AllocatorExit();

// static inline void clflush(char *data, int len)
// {
//     pmem_persist(data, len);
// }