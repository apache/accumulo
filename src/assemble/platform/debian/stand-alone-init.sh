#! /bin/sh
if [ $(id -ur) -ne 0 ]; then
  echo "This script must be run as root" 1>&2
  exit 1
fi
 
./gc-only-init.sh
./monitor-only-init.sh
./tracer-only-init.sh
./master-only-init.sh
./slave-only-init.sh
