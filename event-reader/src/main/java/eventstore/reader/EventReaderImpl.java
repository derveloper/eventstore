package eventstore.reader;

import eventstore.cache.EventCache;
import eventstore.persistence.EventPersistence;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.shared.constants.MessageFields.ERROR_FIELD;
import static eventstore.shared.constants.Messages.NOT_FOUND_MESSAGE;


class EventReaderImpl implements EventReader {
  private final EventPersistence eventPersistence;
  private Logger logger;
  private final EventCache eventCache;

  EventReaderImpl(final Vertx vertx) {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-reader"));
    eventCache = EventCache.createProxy(vertx, "event-cache");
    eventPersistence = EventPersistence.createProxy(vertx, "inmem-event-reader");
  }

  @Override
  public void read(final JsonObject query, final Handler<AsyncResult<JsonArray>> result) {
    logger.debug(String.format("consume read.events: %s", query.encodePrettily()));
    eventCache.read(query, event -> {
      if (event.succeeded()) {
        logger.debug("reply from read.cache.events");
        result.handle(Future.succeededFuture(event.result()));
      }
      else {
        logger.debug("reply from read.cache.events not successful, getting event from db");
        eventPersistence.read(query, persistence -> {
          if (persistence.succeeded()) {
            logger.debug("reply from read.persisted.events");
            result.handle(Future.succeededFuture(persistence.result()));
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
