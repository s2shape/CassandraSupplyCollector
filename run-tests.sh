#!/bin/sh
#docker run --name some-cassandra -d -e CASSANDRA_BROADCAST_ADDRESS=192.168.1.82 -p 7000:7000 cassandra:3

#sleep 10
#sudo docker cp PostgresSupplyCollectorTests/tests/data.sql postgres:/docker-entrypoint-initdb.d/data.sql
#sudo docker exec -u postgres postgres psql postgres postgres -f docker-entrypoint-initdb.d/data.sql
dotnet test
#sudo docker stop postgres
#sudo docker rm postgres
