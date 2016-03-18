package eventstore.monolith;

import eventstore.boundary.HttpApi;
import eventstore.boundary.PushApiVerticle;
import eventstore.boundary.StompBridge;
import eventstore.cache.EventCacheVerticle;
import eventstore.persistence.RethinkDBEventPersistenceVerticle;
import eventstore.reader.ReadEventsVerticle;
import eventstore.writer.WriteEventsVerticle;
import io.vertx.core.Vertx;


public class Eventstore {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    vertx.deployVerticle(new StompBridge());
    vertx.deployVerticle(new PushApiVerticle());
    vertx.deployVerticle(new EventCacheVerticle());
    vertx.deployVerticle(new ReadEventsVerticle());
    vertx.deployVerticle(new WriteEventsVerticle());
    vertx.deployVerticle(new RethinkDBEventPersistenceVerticle());
    vertx.deployVerticle(new HttpApi());
  }
}
