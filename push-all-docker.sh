#!/bin/sh

services='stomp-bridge event-writer event-cache event-reader push-api eventstore.persistence-rethinkdb http-api'

for service in $services; do
    docker push vileda/es-$service
done
