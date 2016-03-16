#!/bin/sh

mvn clean package -DskipTests

vertx run eventstore.boundary.StompBridge \
    -cp stomp-bridge/target/*fat* -cluster -ha -cluster-host 172.28.0.99 --instances 1 &

vertx run eventstore.writer.WriteEventsVerticle \
    -cp event-writer/target/*fat* -cluster -ha -cluster-host 172.28.0.99 --instances 2 &

vertx run eventstore.cache.EventCacheVerticle \
    -cp event-cache/target/*fat* -cluster -ha -cluster-host 172.28.0.99  --instances 2 &

vertx run eventstore.reader.ReadEventsVerticle \
    -cp event-reader/target/*fat* -cluster -ha -cluster-host 172.28.0.99  --instances 2 &

vertx run eventstore.boundary.PushApiVerticle \
    -cp push-api/target/*fat* -cluster -ha -cluster-host 172.28.0.99  --instances 2 &

vertx run eventstore.persistence.RethinkDBEventPersistenceVerticle \
    -cp persistence-rethinkdb/target/persistence-rethinkdb-1.0-SNAPSHOT-fat.jar -cluster -ha -cluster-host 172.28.0.99  --instances 2 &

vertx run eventstore.boundary.HttpApi \
    -cp http-api/target/http-api-1.0-SNAPSHOT-fat.jar -cluster -ha -cluster-host 172.28.0.99  --instances 1 &

wait
