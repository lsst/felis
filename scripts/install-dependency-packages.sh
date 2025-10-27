#!/bin/bash

# This script installs additional packages used by the dependency image but
# not needed by the runtime image, such as additional packages required to
# build Python dependencies.
#
# Since the base image wipes all the apt caches to clean up the image that
# will be reused by the runtime image, we unfortunately have to do another
# apt-get update here, which wastes some time and network.

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for details.
set -euo pipefail

# Display each command as it's run.
set -x

# Tell apt-get we're never going to be able to give manual feedback.
export DEBIAN_FRONTEND=noninteractive

# Update the package listing, so we know what packages exist.
apt-get update

# Install various dependencies that may be required to install vo-cutouts:
#
# build-essential: sometimes needed to build Python modules
# git: required by setuptools_scm
# libffi-dev: sometimes needed to build cffi, a cryptography dependency
apt-get -y install --no-install-recommends build-essential git libpq-dev
