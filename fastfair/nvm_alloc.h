#ifndef _NVM_ALLOC_H
#define _NVM_ALLOC_H

#include <filesystem>
#include <libpmem.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <atomic>
#include <shared_mutex>
#include <iostream>

namespace NVM
{

static inline void *PmemMapFile(const std::string &file_name, const size_t file_size, size_t *len)
{
    int is_pmem;
    std::filesystem::remove(file_name);
    void *pmem_addr_ = pmem_map_file(file_name.c_str(), file_size,
                PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, len, &is_pmem);
#ifdef SERVER
    assert(is_pmem == 1);
#endif
    if (pmem_addr_ == nullptr) {
        perror("BLevel::BLevel(): pmem_map_file");
        exit(1);
    }
    return pmem_addr_;
}

// #define USE_MEM
#ifdef USE_MEM

static inline void Mem_persist(const void *addr, size_t len) {
}

class Alloc {

public:
    Alloc(const std::string &file_name, const size_t file_size) {}

    virtual ~Alloc() {}

    void *alloc(size_t size)
    {
        return malloc(size);
    }

    void Info() {}
    
    void Free(void *p, size_t size) {
        free(p);
    }

    void Free(void *p) {
        free(p);
    }
};
#else

static inline void Mem_persist(const void *addr, size_t len) {
    pmem_persist(addr, len);
}
class Alloc {

public:
    Alloc(const std::string &file_name, const size_t file_size) {
        pmem_file_ = file_name;
        pmem_addr_ = PmemMapFile(pmem_file_, file_size, &mapped_len_);
        current_addr = pmem_addr_;
        used_ = freed_ = 0;
        std::cout << "Map addrs:" <<  pmem_addr_ << std::endl;
        std::cout << "Current addrs:" <<  current_addr << std::endl;
    }

    virtual ~Alloc()
    {
        if(pmem_addr_) {
            pmem_unmap(pmem_addr_, mapped_len_);
        } 
        size_t kb = used_ / 1024;
        size_t mb = kb / 1024;
        std::cout << pmem_file_ << " used: " << used_ << " bytes. (" << mb  << " Mib, " 
                << kb % 1024 << "kib." << " free " <<  freed_ / 1024 / 1024   << " Mib, " 
                << (freed_ / 1024 ) % 1024 << "kib.)" << std::endl;
    }

    void Info()
    {
        size_t kb = used_ / 1024;
        size_t mb = kb / 1024;
        std::cout << pmem_file_ << " used: " << used_ << " bytes. (" << mb  << " Mib, " 
                << kb % 1024 << "kib." << " free " <<  freed_ / 1024 / 1024   << " Mib, " 
                << (freed_ / 1024 ) % 1024 << "kib.)" << std::endl;
    }

    void *alloc(size_t size)
    {
        std::unique_lock<std::mutex> lock(lock_);
        void* p = current_addr;
        used_ += size;
        current_addr = (char *)(current_addr) + size;
        assert(used_ <= mapped_len_);
        // std::cout << "Alloc at pos: " << p << std::endl;
        return p;
        // return malloc(size);
    }

    void Free(void *p, size_t size) {
        if(p == nullptr) return;
        std::unique_lock<std::mutex> lock(lock_);
        // if((char *)p + size == current_addr) {
        //     current_addr = p;
        //     used_ -= size;
        // } else {
            // std::cout << "Free not at pos: " << p << std::endl;
            freed_ += size;
        // }
        // free(p);
    }

    void Free(void *p) {
        // free(p);
    }

private:
    void *pmem_addr_;
    void *current_addr;
    size_t mapped_len_;
    size_t used_;
    size_t freed_;
    std::string pmem_file_;
    static int file_id_;
    std::mutex lock_;
};
#endif
extern Alloc *common_alloc;
extern Alloc *data_alloc;
extern Alloc *data_alloc;

int  env_init();
int  data_init();
void env_exit();
void show_stat();

} // namespace NVM

#endif