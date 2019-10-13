#ifndef BIT_ARRAY_MACROS_H
#define BIT_ARRAY_MACROS_H

#define _TYPESHIFT(arr,word,shift) \
        ((__typeof(*(arr)))((__typeof(*(arr)))(word) << (shift)))

#define bitsetX_wrd(wrdbits,pos) ((pos) / (wrdbits))
#define bitsetX_idx(wrdbits,pos) ((pos) % (wrdbits))

#define bitset64_wrd(pos) ((pos) >> 6ULL)
#define bitset64_idx(pos) ((pos) & 63ULL)

#define bitset2_get(arr,wrd,idx)     (((arr)[wrd] >> (idx)) & 0x1)
#define bitset2_set(arr,wrd,idx)     ((arr)[wrd] |=  _TYPESHIFT(arr,1,idx))
#define bitset2_del(arr,wrd,idx)     ((arr)[wrd] &=~ _TYPESHIFT(arr,1,idx))
#define bitset2_tgl(arr,wrd,idx)     ((arr)[wrd] ^=  _TYPESHIFT(arr,1,idx))
#define bitset2_cpy(arr,wrd,idx,bit) ((arr)[wrd]  = ((arr)[wrd] &~ _TYPESHIFT(arr,1,idx)) | _TYPESHIFT(arr,bit,idx))

#define bitset2_cpy(arr,wrd,idx,bit) ((arr)[wrd]  = ((arr)[wrd] &~ _TYPESHIFT(arr,1,idx)) | _TYPESHIFT(arr,bit,idx))

#define bitset_wrd(arr,pos) bitsetX_wrd(sizeof(*(arr))*8,pos)
#define bitset_idx(arr,pos) bitsetX_idx(sizeof(*(arr))*8,pos)
#define bitset_op(func,arr,pos)      func(arr, bitset_wrd(arr,pos), bitset_idx(arr,pos))
#define bitset_op2(func,arr,pos,bit) func(arr, bitset_wrd(arr,pos), bitset_idx(arr,pos), bit)

#define bitset_op(func,arr,pos)      func(arr, bitset_wrd(arr,pos), bitset_idx(arr,pos))
#define bitset_op2(func,arr,pos,bit) func(arr, bitset_wrd(arr,pos), bitset_idx(arr,pos), bit)

#define bitset_get(arr,pos)     bitset_op(bitset2_get, arr, pos)
#define bitset_set(arr,pos)     bitset_op(bitset2_set, arr, pos)
#define bitset_del(arr,pos)     bitset_op(bitset2_del, arr, pos)
#define bitset_tgl(arr,pos)     bitset_op(bitset2_tgl, arr, pos)
#define bitset_cpy(arr,pos,bit) bitset_op2(bitset2_cpy, arr, pos, bit)

#define bit_array_get(arr,i)      bitset_get((arr)->words, i)
#define bit_array_set(arr,i)      bitset_set((arr)->words, i)
#define bit_array_clear(arr,i)    bitset_del((arr)->words, i)
#define bit_array_toggle(arr,i)   bitset_tgl((arr)->words, i)
#define bit_array_assign(arr,i,c) bitset_cpy((arr)->words,i,c)

#define bit_array_num_of_words(arr) ((arr)->num_of_words)

#define bitmask(nbits,type) ((nbits) ? ~(type)0 >> (sizeof(type)*8-(nbits)): (type)0)
#define bitmask64(nbits) bitmask(nbits,uint64_t)
#define bitmask_merge(a,b,abits) (b ^ ((a ^ b) & abits))

#endif //BIT_ARRAY_MACROS_H
