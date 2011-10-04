#!/bin/bash

for PROTO in `ls -1 *proto`; do protoc --java_out ../java $PROTO; done
