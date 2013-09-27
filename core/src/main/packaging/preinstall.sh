getent group accumulo >/dev/null || groupadd -r accumulo
getent passwd accumulo >/dev/null || useradd -r -g accumulo -d %{_datadir}/accumulo -s /sbin/nologin -c "Apache Accumulo" accumulo
exit 0
