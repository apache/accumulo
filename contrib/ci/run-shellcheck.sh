#! /usr/bin/env bash

set -e
set -x

mapfile -t filestocheck < <(shfmt -f .)
shellcheck -P SCRIPTDIR -x "${filestocheck[@]}"
