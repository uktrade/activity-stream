#!/bin/bash -xe

echo "Waiting for Elasticsearch..."
wget --waitretry=1 --retry-connrefused -O-  http://127.0.0.1:9200/ &> /dev/null
echo "Elasticsearch is running"

# So we have coverage for sub-processes
SITE_PACKAGES_DIR=$(python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
echo "import coverage; coverage.process_startup()" > "${SITE_PACKAGES_DIR}/coverage.pth"
export COVERAGE_PROCESS_START=.coveragerc
python3 -m unittest -v -b
