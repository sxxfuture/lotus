#!/usr/bin/env bash
#
cd ../extern
#
if [ ! -d "filecoin-ffi/.git" ];then
    rm -rf filecoin-ffi
    git clone https://github.com/filecoin-project/filecoin-ffi.git
fi
#
if [ ! -d "serialization-vectors/.git" ];then
    rm -rf serialization-vectors
    git clone https://github.com/filecoin-project/serialization-vectors.git
fi
#
if [ ! -d "test-vectors/.git" ];then
    rm -rf test-vectors
    git clone https://github.com/filecoin-project/test-vectors.git
fi
#
if [ -d "filecoin-ffi/.git" ];then
    cd filecoin-ffi
    git checkout 943e335
    cd ../
fi
#
if [ -d "serialization-vectors/.git" ];then
    cd serialization-vectors
    git checkout 5bfb928
cd ../
fi
#
if [ -d "filecoin-ffi/.git" ];then
    cd test-vectors
    git checkout d9a75a7
cd ../
fi
#
##################
#
#if [ -d "test-vectors" ];then
#    cd test-vectors/gen/extern/
#    #
#    if [ ! -d "fil-blst/.git" ];then
#        rm -rf fil-blst
#        git clone https://github.com/filecoin-project/fil-blst.git
#    fi
#    #
#    if [ ! -d "filecoin-ffi/.git" ];then
#        rm -rf filecoin-ffi
#        git clone https://github.com/filecoin-project/filecoin-ffi.git
#    fi
#    #
#    if [ -d "fil-blst/.git" ];then
#        cd fil-blst
#        git checkout 5f93488
#        cd ../
#    fi
#    #
#    if [ -d "filecoin-ffi/.git" ];then
#        cd filecoin-ffi
#        git checkout f640612
#        cd ../../../../../
#    fi
#fi
echo "clone finish"
# #
# echo "go mod start"
# #
# cd extern/filecoin-ffi
# go mod tidy
# cd ../
# #
# cd serialization-vectors
# go mod tidy
# cd ../
# #
# cd test-vectors
# go mod tidy
# #
# cd gen/extern/fil-blst
# go mod tidy
# cd ../
# #
# cd filecoin-ffi
# go mod tidy
# cd ../../../../../
# #
# go mod tidy
# #
# echo "go mod finish"
# #

