#!/bin/bash
set -ex

generate_proxy_file(){
    cat>proxy<<EOF
export ALL_PROXY=socks5://10.0.0.5:1080
export HTTP_PROXY=http://10.0.0.5:3128
export HTTPS_PROXY=http://10.0.0.5:3128
export NO_PROXY="10.0.0.0/8,172.6.0.0/12,192.168.0.0/16,127.0.0.0/8,localhost,lan,local,sxx.zone,sxxfuture.com,sxxfuture.net"
export all_proxy=\$ALL_PROXY
export http_proxy=\$HTTP_PROXY
export https_proxy=\$HTTPS_PROXY
export no_proxy=\$NO_PROXY
EOF
    cat>noproxy<<EOF
unset ALL_PROXY all_proxy HTTP_PROXY http_proxy HTTPS_PROXY https_proxy NO_PROXY no_proxy
EOF
}

build(){
    mainnet='all lotus-shed lotus-wallet lotus-gateway'
    calibnet='calibnet'
    devnet='2k'
    the_env=$1
    the_path=$2
    if [ "${the_env}" = "devnet" ] ; then
        sed "s/__THE_TARGET_WILL_MODIFY_BY_SCRIPT/${devnet}/g" Dockerfile.sxx > Dockerfile
    elif [ "${the_env}" = "calibnet" ] ; then
        sed "s/__THE_TARGET_WILL_MODIFY_BY_SCRIPT/${calibnet}/g" Dockerfile.sxx > Dockerfile
    else
        sed "s/__THE_TARGET_WILL_MODIFY_BY_SCRIPT/${mainnet}/g" Dockerfile.sxx > Dockerfile
    fi
    which docker
    CONTAINER_NAME=${the_env}$(date +%Y%m%d%H%M%S)
    IMAGE_NAME=lotus:sxx.${the_env}
    docker build -t ${IMAGE_NAME} .
    docker run -dt --name=${CONTAINER_NAME} ${IMAGE_NAME}
    docker cp ${CONTAINER_NAME}:/usr/local/bin ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libdl.so.2 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/librt.so.1 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libgcc_s.so.1 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libutil.so.1 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libltdl.so.7 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libnuma.so.1 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libhwloc.so.5 ${the_path}
    docker cp ${CONTAINER_NAME}:/lib/libOpenCL.so.1 ${the_path}
    docker stop ${CONTAINER_NAME}
    docker rm -f ${CONTAINER_NAME}
}

if [ $# -lt 2 ] ; then
echo $0 '<devnet|calibnet|mainnet> <path>'
exit 1
fi
generate_proxy_file
build $1 $2
