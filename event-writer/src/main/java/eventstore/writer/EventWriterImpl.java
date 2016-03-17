package eventstore.writer;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.shared.constants.Addresses.CACHE_EVENTS_ADDRESS;
import static eventstore.shared.constants.Addresses.PERSIST_EVENTS_ADDRESS;


class EventWriterImpl implements EventWriter {
  private final Logger logger;
  private final EventBus eventBus;

  EventWriterImpl(EventBus eventBus) {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-writer"));
    this.eventBus = eventBus;
  }

  @Override
  public void write(JsonArray events) {
    logger.debug(String.format("consume write.events %s", events.encodePrettily()));
    eventBus.send(PERSIST_EVENTS_ADDRESS, events, messageAsyncResult -> {
      if (messageAsyncResult.succeeded()) {
        eventBus.publish(CACHE_EVENTS_ADDRESS, events);
      }
    });
  }
}
