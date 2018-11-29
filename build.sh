#!/usr/bin/env bash
python setup.py sdist
cp dist/$(python setup.py --fullname).tar.gz dist/felis.tar.gz
