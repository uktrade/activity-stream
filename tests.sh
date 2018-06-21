#!/bin/bash -xe

# So we have coverage for sub-processes
SITE_PACKAGES_DIR=$(python -c "import site; print(site.getsitepackages()[0])")
echo "import coverage; coverage.process_startup()" > "${SITE_PACKAGES_DIR}/coverage.pth"
export COVERAGE_PROCESS_START=.coveragerc
python -m unittest
