#!/bin/bash -xe

export DJANGO_SETTINGS_MODULE="conf.settings.test"
python manage.py test core
