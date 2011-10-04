#! /usr/bin/env bash

. continuous-env.sh

pssh -h batch_walkers.txt "pkill -f [o]rg.apache.accumulo.server.test.continuous.ContinuousBatchWalker" < /dev/null

