#!/bin/bash
cleanup ()
{
kill -s SIGTERM $!
exit 0
}

trap cleanup SIGINT
trap cleanup SIGQUIT
trap cleanup SIGTSTP

echo "Starting cassandra..."
./docker-entrypoint.sh cassandra -f &

sleep 30

echo "Executing init scripts"
ls -al /docker-entrypoint-initdb.d
for f in /docker-entrypoint-initdb.d/*; do case "$f" in *.sh) echo "$0: running $f"; . "$f" ;; *.sql) echo "$0: running $f" && until cqlsh -f "$f"; do >&2 echo "Cassandra is unavailable - sleeping"; sleep 2; done & ;; *) echo "$0: ignoring $f" ;; esac done
echo "Startup finished"

fg

for (( ; ; ))
do
   sleep 1
done
