#!/bin/sh


basedir=`pwd`
mvn clean package -DskipTests

services='stomp-bridge event-writer event-cache event-reader push-api persistence-rethinkdb http-api'

for service in $services; do
    cd $service && docker build -t eventstore/$service . && cd $basedir
done
