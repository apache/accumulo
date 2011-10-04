#! /usr/bin/env bash

. continuous-env.sh

pssh -h ingesters.txt "pkill -f [o]rg.apache.accumulo.server.test.continuous.ContinuousIngest" < /dev/null

