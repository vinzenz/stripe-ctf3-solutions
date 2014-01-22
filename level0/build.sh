#!/bin/sh

g++ hasher.cpp -o hasher -std=c++11 $@
./hasher ./words.dat
gcc -o level0 test.c -std=c99 -O9 -msse2 -march=native -mtune=native $@
strip level0
