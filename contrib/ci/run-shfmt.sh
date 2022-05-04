#! /usr/bin/env bash

set -e
set -x

shfmt -ln bash -l -d -i 2 -ci -s .
