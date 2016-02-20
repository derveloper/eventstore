# eventstore

event sourcing database with STOMP and HTTP API.
events are stored in a RethinkDB.
STOMP and HTTP are implemented using vertx.io

## API

Currently, there are two APIs: HTTP and STOMP.

### HTTP API

#### Writing events

Write events by posting them via HTTP.
```
curl -v -d '[{"id": "testid123", "eventType": "barfoo", "data": { "foo": "bar" } }]' loclahost:8080/stream/testbar1234
```

#### Fetching events

Fetch events using a `GET` HTTP request.
You can pass query arguments to the URL for filtering.
```
curl "http://localhost/stream/testbar1234?eventType=barfoo"
```

### STOMP

#### Writing events

Not implemented yet.

#### Subscribing to events

Just use the same URL as in the HTTP API to be notified when a new event is written.

## Building

At the moment, you need to have a running RethinkDB instance on `localhost` if `-DskipTests` is not set.
To build the jar simply run `mvn clean package`

## Running

To run eventstore, execute the following
```
java -DEVENTSTORE_HTTP_PORT=8080 -jar target/eventstore-1.0-SNAPSHOT-fat.jar
```

You can also pass the RethinkDB host by setting the `EVENTSTORE_RETHINKDB_ADDRESS` environment variable.
The default is `localhost`.
If you omit the `EVENTSTORE_HTTP_PORT` property, eventstore will grab a random free port.

## license
```
Copyright 2016 vileda

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


