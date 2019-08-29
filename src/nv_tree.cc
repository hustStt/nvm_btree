#include "nv_tree.h"

int PLeafNode::BinarySearch(const char * &key, int start, int end) {
    if(end - start <= 4) {
        while(KeyCompare(key, GetKey(start)) > 0) {
            start ++;
        }
        return start;
    }
    int binaryOffset = (start + end) / 2;
    int res = KeyCompare(key, GetKey(binaryOffset));
    if(res == 0) {
        return binaryOffset;
    } else if(res < 0) {
        return BinarySearch(key, start, binaryOffset - 1);
    } else {
        return BinarySearch(key, binaryOffset + 1, end);
    }
}

int IN::BinarySearch(const char * &key, int start, int end) {
    if(end - start <= 4) {
        while(KeyCompare(key, GetKey(start)) > 0) {
            start ++;
        }
        return start;
    }
    int binaryOffset = (start + end) / 2;
    int res = KeyCompare(key, GetKey(binaryOffset));
    if(res == 0) {
        return binaryOffset;
    } else if(res < 0) {
        return BinarySearch(key, start, binaryOffset - 1);
    } else {
        return BinarySearch(key, binaryOffset + 1, end);
    }
}