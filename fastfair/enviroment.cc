#include "nvm_alloc.h"


namespace NVM
{
Alloc *common_alloc = nullptr;
Alloc *data_alloc = nullptr;

#ifdef SERVER
const size_t common_alloc_size = 4 *1024 * 1024 * 1024UL;
const size_t data_alloc_size = 50 * 1024 * 1024 * 1024UL;
#else
const size_t common_alloc_size = 1024 * 1024 * 1024UL;
const size_t data_alloc_size = 4 * 1024 * 1024 * 1024UL;
#endif

int data_init() {
    if(!data_alloc) {
#ifndef USE_MEM
        data_alloc  = new  NVM::Alloc("/mnt/pmem1/data", data_alloc_size);
#endif
    }
    return 0;
}

void env_exit()
{
    if(data_alloc) delete data_alloc;
    if(common_alloc) delete common_alloc;
}

void show_stat()
{
    if(data_alloc)  data_alloc->Info();
    if(common_alloc)  common_alloc->Info();
}
} // namespace NVM
