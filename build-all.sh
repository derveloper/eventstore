#!/bin/sh


basedir=`pwd`
mvn clean package -DskipTests

services='stomp-bridge event-writer event-cache event-reader push-api persistence/persistence-rethinkdb http-api'

for service in $services; do
    cd $service && docker build -t vileda/es-$service . && cd $basedir
done
