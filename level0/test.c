#include "murmurhash3.hpp"
#include "hashed.h"
#include <memory.h>
#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include "hash_index.h"

extern int fputc_unlocked (int __c, FILE *__stream);
extern size_t fwrite_unlocked (const void *__restrict __ptr, size_t __size,
			       size_t __n, FILE *__restrict __stream);

FORCE_INLINE int bsearch(uint64_t const * b, int len, uint64_t c)
{
    int middle = len >> 1;
    uint64_t const * m = b + middle;
    uint64_t mc = 0;
    while(len > 0) {
        middle = len >> 1;
        m = b + middle;
        mc = *m;
        if(c == mc) {
            return 1;
        }
        if(c < mc) {
            len = middle;
        }
        else {
            b = m + 1;
            len -= (middle + 1);
        }
    }
    return 0;
}


FORCE_INLINE int known(char const * ptr, int l) {
uint64_t known_buf[2]={};
    int const bl =  (l & 16) + 16;
    MurmurHash3_x64_128(ptr, bl, HASH_SEED, known_buf);
    uint64_t hashres = known_buf[0];
    int idx = extract_index1(hashres);
    uint64_t const * b = BUCKET_DATA[idx];
    int result = 0;
    uint64_t f = 0;

    for(int i = 0; i < BUCKET_SIZES[idx]; ++i) {
        f = !(*b ^ hashres); result = (result << 1) | f;
        ++b;
    }
    return 0 | (-(result != 0) & 1);
}

#define CHUNK_SIZE 16
#define handle(n)                               \
    c = pchunk[last_len - n];                   \
    switch(c) {                                 \
    case ' ':                                   \
    case '\n':                                  \
        found = !known(lline, len);             \
        fwrite(&bchar, found, found, stdout);   \
        fwrite(line, len, 1, stdout);           \
        fwrite(&echar, found, found, stdout);   \
        memset(line, 0, 32);                    \
        memset(lline, 0, 32);                   \
        len = 0;                                \
        putchar(c);                             \
    break;                                      \
    default:                                    \
        lline[len] = tolower(c);                \
        line[len] = c;                          \
        ++len;                                  \
    }


int main(int argc, char const ** argv) {
    char line[32] = {};
    char lline[32] = {};
    char bchar = '<', echar = '>';
    int len = 0;
    int found = 0;
    char c = 0;
    size_t chunk_idx = 0;
    uint64_t chunk[2] = {};
    char * pchunk = (char*)&chunk;
    size_t last_len = CHUNK_SIZE;
    while(last_len == CHUNK_SIZE) {
        chunk[0] = chunk[1] = 0;
        last_len = fread(pchunk, 1, CHUNK_SIZE, stdin);
        switch(last_len){
        case 16: handle(16);
        case 15: handle(15);
        case 14: handle(14);
        case 13: handle(13);
        case 12: handle(12);
        case 11: handle(11);
        case 10: handle(10);
        case 9: handle(9);
        case 8: handle(8);
        case 7: handle(7);
        case 6: handle(6);
        case 5: handle(5);
        case 4: handle(4);
        case 3: handle(3);
        case 2: handle(2);
        case 1: handle(1);
        default: break;
        }
    }
    return 0;
}
