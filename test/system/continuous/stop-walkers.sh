#! /usr/bin/env bash

. continuous-env.sh

pssh -h walkers.txt "pkill -f [o]rg.apache.accumulo.server.test.continuous.ContinuousWalk" < /dev/null

