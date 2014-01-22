#ifndef __HASH_INDEX_H__
#define __HASH_INDEX_H__

#ifndef HASH_SEED
#define HASH_SEED 0x0A0B0C0DUL
#endif

#define extract_index1(v) \
    (uint16_t)(((v) & 0xFFFF000000000000ull) >> 48)

#define extract_index2(v) \
   (uint16_t)(((v) & 0x000000000FFFF00000ull) >> 20)

#endif /* !__HASH_INDEX_H__ */

