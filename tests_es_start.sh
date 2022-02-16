#!/bin/bash -xe

docker run --rm --name activity-stream-elasticsearch -d -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" \
    --entrypoint bash \
    opensearchproject/opensearch:1.1.0 \
    -c 'rm -r -f /usr/share/opensearch/plugins/opensearch-security && exec /usr/share/opensearch/opensearch-docker-entrypoint.sh'
