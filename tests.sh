#!/bin/bash -xe

echo "Waiting for Elasticsearch..."
wget --waitretry=1 --retry-connrefused -O-  http://127.0.0.1:9200/ &> /dev/null
echo "Elasticsearch is running"

# So we have coverage for sub-processes
SITE_PACKAGES_DIR=$(python -c "import site; print(site.getsitepackages()[0])")
echo "import coverage; coverage.process_startup()" > "${SITE_PACKAGES_DIR}/coverage.pth"
export COVERAGE_PROCESS_START=.coveragerc
cp -r -f shared core
python -m unittest -v core.app.tests.TestApplication.test_get_returns_feed_data
