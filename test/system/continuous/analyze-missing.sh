#! /usr/bin/env bash

. continuous-env.sh

./analyze-missing.pl $ACCUMULO_HOME $CONTINUOUS_LOG_DIR $USER $PASS

