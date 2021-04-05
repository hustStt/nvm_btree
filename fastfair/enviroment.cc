#include "nvm_alloc.h"


namespace NVM
{
Alloc *data_alloc = nullptr;


const size_t data_alloc_size = 100 * 1024 * 1024 * 1024UL;


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
}

void show_stat()
{
    if(data_alloc)  data_alloc->Info();
}
} // namespace NVM
