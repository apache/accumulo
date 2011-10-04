#! /usr/bin/env bash

. continuous-env.sh

pkill -f org.apache.accumulo.server.test.continuous.ContinuousStatsCollector

