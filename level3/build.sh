#!/bin/sh

rm -rf test/data/input
mkdir -p test/data
mkdir -p /tmp/lvl3-foobar/input
rm -rf /tmp/lvl3-foobar/input/*
ln -sf /tmp/lvl3-foobar/input test/data/input


