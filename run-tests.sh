#!/bin/sh
dotnet build
sudo docker rm -f -v cassandra
sudo docker run --name cassandra -v /tmp:/var/lib/cassandra -d -e CASSANDRA_BROADCAST_ADDRESS=127.0.0.1 -p 9042:9042 cassandra:3
sleep 10
sudo cp CassandraSupplyCollectorTests/tests/data.sql /tmp
sudo docker exec -i cassandra cqlsh -f /var/lib/cassandra/data.sql
dotnet test
sudo docker stop cassandra
sudo docker rm cassandra
sudo rm /tmp/data.sql
