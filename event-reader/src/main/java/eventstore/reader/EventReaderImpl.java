package eventstore.reader;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.shared.constants.Addresses.*;
import static eventstore.shared.constants.MessageFields.ERROR_FIELD;
import static eventstore.shared.constants.Messages.NOT_FOUND_MESSAGE;


public class EventReaderImpl implements EventReader {
  private EventBus eventBus;
  private Logger logger;

  public EventReaderImpl(EventBus eventBus) {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-reader"));
    this.eventBus = eventBus;
  }

  @Override
  public void read(final JsonObject query, final Handler<AsyncResult<JsonArray>> result) {
    logger.debug(String.format("consume read.events: %s", query.encodePrettily()));
    final DeliveryOptions cacheDeliveryOptions = new DeliveryOptions()
        .setSendTimeout(200);
    eventBus.send(READ_CACHE_EVENTS_ADDRESS, query, cacheDeliveryOptions, messageAsyncResult -> {
      if (messageAsyncResult.succeeded()) {
        logger.debug("reply from read.cache.events");
        result.handle(Future.succeededFuture((JsonArray) messageAsyncResult.result().body()));
      }
      else {
        logger.debug("reply from read.cache.events not successful, getting event from db");
        eventBus.send(READ_PERSISTED_EVENTS_ADDRESS, query, dbMessageAsyncResult -> {
          if (dbMessageAsyncResult.succeeded()) {
            logger.debug("reply from read.persisted.events");
            eventBus.send(WRITE_CACHE_EVENTS_ADDRESS, dbMessageAsyncResult.result().body());
            result.handle(Future.succeededFuture((JsonArray) dbMessageAsyncResult.result().body()));
          }
          else {
            logger.error("reply from read.persisted.events failed!");
            result.handle(Future.failedFuture(new JsonObject().put(ERROR_FIELD, NOT_FOUND_MESSAGE).encodePrettily()));
          }
        });
      }
    });
  }
}
