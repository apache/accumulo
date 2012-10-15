#! /bin/sh
if [ $(id -ur) -ne 0 ]; then
  echo "This script must be run as root" 1>&2
  exit 1
fi

if [ ! -f /etc/default/accumulo ]; then
  mkdir -p /etc/default
  touch /etc/default/accumulo
fi

if ! grep "ACCUMULO_MONITOR_USER=" /etc/default/accumulo  >> /dev/null ; then
  echo "ACCUMULO_MONITOR_USER=accumulo_monitor" >> /etc/default/accumulo
fi
 
if ! id -u accumulo_monitor >/dev/null 2>&1; then
  if ! egrep "^accumulo:" /etc/group > /dev/null; then
    groupadd accumulo
  fi 
  useradd -d /usr/lib/accumulo -g accumulo accumulo_monitor
fi

install -m 0755 -o root -g root init.d/accumulo-monitor /etc/init.d/
update-rc.d accumulo-monitor defaults 

