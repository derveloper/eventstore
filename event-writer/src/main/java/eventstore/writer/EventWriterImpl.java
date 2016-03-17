package eventstore.writer;

import eventstore.cache.EventCache;
import eventstore.persistence.EventPersistence;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


class EventWriterImpl implements EventWriter {
  private final Logger logger;
  private final EventBus eventBus;
  private final EventPersistence eventPersistence;
  private final EventCache eventCache;

  EventWriterImpl(final EventBus eventBus, final Vertx vertx) {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-writer"));
    this.eventBus = eventBus;
    eventPersistence = EventPersistence.createProxy(vertx, "event-persistence");
    eventCache = EventCache.createProxy(vertx, "event-cache");
  }

  @Override
  public void write(JsonArray events) {
    logger.debug(String.format("consume write.events %s", events.encodePrettily()));
    eventPersistence.write(events, event -> {
      if (!events.isEmpty()) {
        final JsonObject first = events.getJsonObject(0);
        String address =
            String.format("/stream/%s?eventType=%s", first.getString("streamName"), first.getString("eventType"));
        eventBus.publish(address, events);
      }
      eventCache.write(events);
    });
  }
}
