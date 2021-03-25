#include"utility.h"
#include<string>
#include<fstream>
#include "../include/ycsb/core/utils.h"

using namespace std;

uint64_t calLeafSize() {
    int n = LEAF_DEGREE * 2;
    int bitArrNum = (n + 7) / 8;
    // Leaf : | bitmap | pNext | fingerprints array | KV array |
    uint64_t size = bitArrNum + sizeof(PPointer) 
                  + n * sizeof(Byte)
                  + n * (sizeof(Key) + sizeof (Value));
    return size;
}

uint64_t countOneBits(Byte b) {
    uint64_t count = 0;
    while(b != 0) {
        count++;
        b = b & (b - 1);
    }
    return count;
}

const uint64_t kFNVOffsetBasis64_ = 0xCBF29CE484222325;
const uint64_t kFNVPrime64_ = 1099511628211;

// func that generates the fingerprints
Byte keyHash(Key k) {
    uint64_t hash = kFNVOffsetBasis64_;
    uint64_t octet = k & 0x00ff;
    hash = hash ^ octet;
    hash = hash * kFNVPrime64_;
    return hash & 0x00ff;
}

bool PPointer::operator==(const PPointer p) const {
    if (this->fileId == p.fileId && this->offset == p.offset) {
        return true;
    } else {
        return false;
    }
}

// get the pNext of the leaf in the leaf file
PPointer getPNext(PPointer p) {
    string leafPath = DATA_DIR + to_string(p.fileId);
    ifstream file(leafPath.c_str(), ios::in|ios::binary);
    PPointer t_p;
    t_p.fileId = 0;
    t_p.offset = 0;
    if (!file.is_open()) {
        return t_p;
    }
    int len = (LEAF_DEGREE * 2 + 7) / 8 + p.offset;
    file.seekg(len, ios::beg);
    file.read((char*)&(t_p), sizeof(PPointer));
    return t_p;
}