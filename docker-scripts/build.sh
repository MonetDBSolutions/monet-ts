#!/bin/bash -e

BRANCH=$1
if [ "${BRANCH}" == "" ]; then
    BRANCH="master"
fi

echo "Building version guardian:${BRANCH}"

docker build \
    --tag "monetdb/guardian:${BRANCH}" \
    --no-cache=true .
docker tag monetdb/guardian:${BRANCH} monetdb/guardian:latest

