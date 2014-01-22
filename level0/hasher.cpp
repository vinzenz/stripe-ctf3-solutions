#include "murmurhash3.hpp"
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <memory.h>
#include <random>

#include "hash_index.h"

struct hashinfo {
    uint64_t a;
    uint64_t b;
};

bool operator<(hashinfo const & a, hashinfo const & b) {
    return a.a < b.a;
}

bool operator==(hashinfo const & a, hashinfo const & b) {
    return a.a == b.a;
}

size_t biggest_bucket = 0;


#define OFS_DEF(x) ofstream ofs((x))
#define OFS ofs
int main(int argc, char const ** argv) {
    using namespace std;

    ifstream ifs(argc > 1 ? argv[1] : "test/data/words-6b898d7c48630be05b72b3ae07c5be6617f90d8e");
    vector<string> words;
    string word;
    while(ifs >> word) {
        bool drop = false;
        for(char c : word) {
            if(c >= 'A' && c <= 'Z') {
                drop = true;
                break;
            }
        }
        if(!drop)
            words.push_back(word);
    }
    printf("%d words indexed\n", words.size());
    vector<hashinfo> hashes;
    unordered_map<uint64_t, std::string> wordtable;
    for(auto w : words) {
        hashinfo i;
        char buf[32] = {};
        memset(&i, 0, sizeof(hashinfo));
        memcpy(buf, w.c_str(), w.size());
        MurmurHash3_x64_128(buf, (strlen(w.c_str()) & 16) + 16, HASH_SEED, &i);
        wordtable[i.a] += std::string(buf) + ",";
        hashes.push_back(i);
    }

    sort(hashes.begin(), hashes.end());

    OFS_DEF("hashed.h");
    vector<uint64_t> arr[0x10000] ={};
    vector<uint64_t> arr2[0x10000] ={};
    for(auto h : hashes) {
        uint16_t index1 = extract_index1(h.a);
        uint16_t index2 = extract_index2(h.a);
        arr[index1].push_back(h.a);
        arr2[index2].push_back(h.a);
    }
    for(size_t i = 0; i < 0x10000; ++i) {
        if(!arr[i].empty()){
            OFS <<  "uint64_t BUCKET_DATA_" <<  std::setfill('0') << setw(4) << i << "[] = {\n";
            sort(arr[i].begin(), arr[i].end());
            for(auto v : arr[i]) {
                OFS <<  "     0x" << setfill('0') << setw(16) << hex << v << ", // " << wordtable[v] << "\n";
            }
            OFS <<  "};\n";
        }
    }
    OFS <<  "uint64_t * BUCKET_DATA[0x10000] = {\n";
    for(size_t i = 0; i < 0x10000; ++i) {
        if(arr[i].empty())
            OFS <<  "0,";
        else
            OFS <<  "BUCKET_DATA_" << setfill('0') << setw(4) << i << ",";
    }
    OFS <<  "};\n" << dec << "uint32_t BUCKET_SIZES[0x10000] = {\n";
    biggest_bucket = 0;
    for(auto x : arr) {
        if(x.size() > biggest_bucket) biggest_bucket = x.size();
        OFS <<  x.size() << ',';
    }
    OFS <<  "};\n";
    printf("Biggest bucket size: %llu entries\n", biggest_bucket);
    return 0;
}


