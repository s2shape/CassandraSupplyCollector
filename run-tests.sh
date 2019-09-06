#!/bin/sh
docker run --name cassandra -d -e CASSANDRA_BROADCAST_ADDRESS=127.0.0.1 -p 9042:9042 cassandra:3
docker cp CassandraSupplyCollectorTests/tests/data.sql cassandra:/data.sql
sleep 10
docker exec -i cassandra cqlsh -f /data.sql

export CASSANDRA_HOST=localhost
export CASSANDRA_KEYSPACE=test

dotnet build
dotnet test

docker stop cassandra
docker rm cassandra
