#! /bin/sh
if [ $(id -ur) -ne 0 ]; then
  echo "This script must be run as root" 1>&2
  exit 1
fi

if [ ! -f /etc/default/accumulo ]; then
  mkdir -p /etc/default
  touch /etc/default/accumulo
fi

if ! grep "ACCUMULO_USER=" /etc/default/accumulo >> /dev/null ; then
  echo "ACCUMULO_USER=accumulo" >> /etc/default/accumulo
fi

if ! id -u accumulo >/dev/null 2>&1; then
  groupArg="U"
  if egrep "^accumulo:" /etc/group >> /dev/null; then
    groupArg="g accumulo"
  fi
  useradd -$groupArg -d /usr/lib/accumulo accumulo
fi

install -m 0755 -o root -g root init.d/accumulo-gc /etc/init.d/
update-rc.d accumulo-gc start 21 2 3 4 5 . stop 20 0 1 6 .
