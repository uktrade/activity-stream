#!/bin/bash -xe

docker run --rm --name activity-stream-redis -d -p 6379:6379 redis:4.0.10
