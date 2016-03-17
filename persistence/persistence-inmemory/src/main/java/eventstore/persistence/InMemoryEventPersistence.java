package eventstore.persistence;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.LinkedList;
import java.util.List;


class InMemoryEventPersistence implements EventPersistence {
  private final List<JsonObject> store = new LinkedList<>();
  private Logger logger;

  InMemoryEventPersistence() {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "inmem-event-reader"));
  }

  @Override
  public void read(JsonObject query, Handler<AsyncResult<JsonArray>> result) {
    logger.debug(String.format("consume read.persisted.events: %s", query.encodePrettily()));
    result.handle(Future.succeededFuture(new JsonArray(Json.encode(store))));
  }

  @Override
  public void write(JsonArray events, Handler<AsyncResult<Boolean>> result) {
    saveEventIfNotDuplicated(events);
    result.handle(Future.succeededFuture(true));
  }

  private void saveEventIfNotDuplicated(final JsonArray body) {
    //noinspection unchecked
    store.addAll(body.getList());
    logger.debug(String.format("persisted %s", body.encodePrettily()));
  }
}
