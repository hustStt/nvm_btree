#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_bptree.h"

#define NODEPATH   "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"

const size_t NVM_NODE_SIZE = 45 * (1ULL << 30);           // 45GB
const size_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB


int main(int argc, char *argv[]) {
    btree *bt = new btree();

    bt->Initial(NODEPATH, NVM_NODE_SIZE, VALUEPATH, NVM_VALUE_SIZE);

    bt->PrintInfo();

    delete btree;
}