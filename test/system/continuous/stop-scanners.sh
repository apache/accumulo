#! /usr/bin/env bash

. continuous-env.sh

pssh -h scanners.txt "pkill -f [o]rg.apache.accumulo.server.test.continuous.ContinuousScanner" < /dev/null

