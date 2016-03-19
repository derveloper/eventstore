#!/bin/sh

MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
mvn clean package -T 1C -DskipTests --offline

vertx run eventstore.boundary.StompBridge \
    -cp stomp-bridge/target/*fat* -cluster -ha -cluster-host 172.17.0.4 --instances 1 &

vertx run eventstore.writer.WriteEventsVerticle \
    -cp event-writer/target/*fat* -cluster -ha -cluster-host 172.17.0.4 --instances 1 &

vertx run eventstore.cache.EventCacheVerticle \
    -cp event-cache/target/*fat* -cluster -ha -cluster-host 172.17.0.4  --instances 1 &

vertx run eventstore.reader.ReadEventsVerticle \
    -cp event-reader/target/*fat* -cluster -ha -cluster-host 172.17.0.4  --instances 1 &

vertx run eventstore.boundary.PushApiVerticle \
    -cp push-api/target/*fat* -cluster -ha -cluster-host 172.17.0.4  --instances 1 &

vertx run eventstore.persistence.RethinkDBEventPersistenceVerticle \
    -cp persistence/persistence-rethinkdb/target/persistence-rethinkdb-1.0-SNAPSHOT-fat.jar \
    -cluster -ha -cluster-host 172.17.0.4  --instances 1 &

vertx run eventstore.boundary.HttpApi \
    -cp http-api/target/http-api-1.0-SNAPSHOT-fat.jar -cluster -ha -cluster-host 172.17.0.4  --instances 1 &

wait
