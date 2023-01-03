#!/bin/bash -xe
python -m core.app.app_incoming &
python -m core.app.app_outgoing &
wait