package eventstore.cache;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static eventstore.shared.constants.MessageFields.*;
import static eventstore.shared.constants.Messages.NOT_FOUND_MESSAGE;


class EventCacheImpl implements EventCache {
  private final Map<String, Map<String, JsonObject>> eventCache = new LinkedHashMap<>();
  private Logger logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-cache"));

  @Override
  public void read(JsonObject query, Handler<AsyncResult<JsonArray>> result) {
    final String streamName = (String) query.remove(EVENT_STREAM_NAME_FIELD);
    logger.debug(String.format("consume read.cache.events: %s", query.encodePrettily()));
    if (query.containsKey(EVENT_ID_FIELD)) {
      if (eventCache.containsKey(streamName) &&
          eventCache.get(streamName).containsKey(query.getString(EVENT_ID_FIELD))) {
        result.handle(Future.succeededFuture(new JsonArray().add(
            new JsonObject(Json.encode(eventCache.get(streamName).get(query.getString(EVENT_ID_FIELD)))))));
      }
      else {
        result.handle(Future.failedFuture(new JsonObject().put(ERROR_FIELD, NOT_FOUND_MESSAGE).encodePrettily()));
      }
    }
    else if (!query.isEmpty()) {
      result.handle(Future.failedFuture(new JsonObject().put(ERROR_FIELD, NOT_FOUND_MESSAGE).encodePrettily()));
    }
    else {
      final JsonArray jsonArray = new JsonArray();
      if (eventCache.containsKey(streamName)) {
        eventCache.get(streamName).values().forEach(jsonArray::add);
      }
      result.handle(Future.succeededFuture(jsonArray));
    }
  }

  @Override
  public void write(JsonArray events) {
    logger.debug("writing to cache");
    events.forEach(o -> {
      final JsonObject object = (JsonObject) o;
      final String streamName = (String) object.remove(EVENT_STREAM_NAME_FIELD);
      final String id = object.getString(EVENT_ID_FIELD);
      eventCache.putIfAbsent(streamName, new LinkedHashMap<>());
      if (!eventCache.get(streamName).containsKey(id)) {
        eventCache.get(streamName).put(id, object);
      }
    });
  }
}
