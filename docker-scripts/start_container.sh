#!/bin/bash

TAG=$1

if [ "${TAG}" == "" ]; then
    TAG="latest"
fi

docker run --rm -ti -P --name guardian monetdb/guardian:${TAG}
