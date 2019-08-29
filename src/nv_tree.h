#include <climits>
#include <future>
#include <mutex>

#include "nvm_common.h"

const int NTMAX_WAY = 4;
const int NTLeaf_Elements = 10;

enum OpFlag {
    OpInsert = 0;

};

class Element {
    uint8_t flag;
    char key[NVM_KeySize];
    void *value;
};

class LeafNode {
    int16_t nElements;
    Element elements[NTLeaf_Elements];
};


class PLeafNode {
    int16_t n_keys;
    char m_key[NTMAX_WAY][NVM_KeySize];
    LeafNode *LNs[NTMAX_WAY + 1];

public:
    int BinarySearch(const char * &key, int start, int end);
    int BinarySearch(const char * &key) {
        BinarySearch(key, 0, n_keys - 1);
    }

    char *GetKey(int off) {
        return m_key[off];
    }

    char *SetKey_nodrain(int off, const char *key) {
        memcpy(m_key[off], key, NVM_KeySize);
    }
};


class IndexNode {
    int16_t n_keys;
    char m_key[NTMAX_WAY *( 2 + 1)][NVM_KeySize];

public:
    int BinarySearch(const char * &key, int start, int end);
    int BinarySearch(const char * &key) {
        BinarySearch(key, 0, n_keys - 1);
    }

    char *GetKey(int off) {
        return m_key[off];
    }

    char *SetKey_nodrain(int off, const char *key) {
        memcpy(m_key[off], key, NVM_KeySize);
    }
};

class NVTree {
    IndexNode *index;
    PLeafNode *pNode;
    int MaxIndex;

};