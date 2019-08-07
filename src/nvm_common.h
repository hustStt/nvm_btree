#pragma once 

#include <cstring>
#include <string>
#include <libpmem.h>
// #include "statistic.h"

namespace scaledkv {
    const int NVM_NodeSize = 256;
    const int NVM_KeySize = 8;
    const int NVM_ValueSize = 1024;
    const int NVM_PointSize = 8;
    const int NVM_KeyBuf = NVM_KeySize + NVM_PointSize;
    const int EntryInterval = 128;
    const double Double1 = 1.0;
    // Statistic stats;
    static inline int KeyCompare(const void *key1, const void *key2) {
        return memcmp(key1, key2, NVM_KeySize);
    }

    static inline uint64_t char8toint64(const char *key) {
        uint64_t value = ((((uint64_t)key[0]) & 0xff) << 56) | ((((uint64_t)key[1]) & 0xff) << 48) |
               ((((uint64_t)key[2]) & 0xff) << 40) | ((((uint64_t)key[3]) & 0xff) << 32) |
               ((((uint64_t)key[4]) & 0xff) << 24) | ((((uint64_t)key[5]) & 0xff) << 16) |
               ((((uint64_t)key[6]) & 0xff) << 8)  | (((uint64_t)key[7]) & 0xff);
        return value;

    }

    static inline void fillchar8wirhint64(char *key, uint64_t value) {
        key[0] = ((char)(value >> 56)) & 0xff;
        key[1] = ((char)(value >> 48)) & 0xff;
        key[2] = ((char)(value >> 40) )& 0xff;
        key[3] = ((char)(value >> 32)) & 0xff;
        key[4] = ((char)(value >> 24)) & 0xff;
        key[5] = ((char)(value >> 16)) & 0xff;
        key[6] = ((char)(value >> 8)) & 0xff;
        key[7] = ((char)value) & 0xff;
    }

    static inline double CaculateCDF(const char *key, uint64_t min_key, uint64_t max_key) {
        uint64_t tmp = char8toint64(key);
        if(tmp < min_key) {
            return -Double1;
        } else if(tmp > max_key) {
            return Double1 * 2;
        }
        double CDFI = (Double1 * (tmp - min_key)) / (max_key - min_key);
        return CDFI ;
    }
}

// #define SBH_DEBUG
#ifdef SBH_DEBUG
#define SBH_PRINT(format, a...) printf("DEBUG: %-20s %4d:"#format"\n", __FUNCTION__, __LINE__, ##a)
#else 
#define SBH_PRINT(format, a...)
#endif