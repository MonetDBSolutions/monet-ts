#!/bin/bash

TAG=$1
if [ "${TAG}" == "" ]; then
    TAG="master"
fi

echo "pushing monetdb/guardian:${TAG} ..."

docker push monetdb/guardian:${TAG}
docker push monetdb/guardian:latest

echo "Done"
