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


class EventCacheImpl implements EventCache {
  private final Map<String, Map<String, JsonObject>> eventCache = new LinkedHashMap<>();
  private Logger logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-cache"));

  @Override
  public void read(JsonObject query, Handler<AsyncResult<JsonArray>> result) {
    final String streamName = (String) query.remove("streamName");
    logger.debug(String.format("consume read.cache.events: %s", query.encodePrettily()));
    if (query.containsKey("id")) {
      if (eventCache.containsKey(streamName) &&
          eventCache.get(streamName).containsKey(query.getString("id"))) {
        result.handle(Future.succeededFuture(new JsonArray().add(
            new JsonObject(Json.encode(eventCache.get(streamName).get(query.getString("id")))))));
      }
      else {
        result.handle(Future.failedFuture(new JsonObject().put("error", "not found").encodePrettily()));
      }
    }
    else if (!query.isEmpty()) {
      result.handle(Future.failedFuture(new JsonObject().put("error", "not found").encodePrettily()));
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
      final String streamName = (String) object.remove("streamName");
      final String id = object.getString("id");
      eventCache.putIfAbsent(streamName, new LinkedHashMap<>());
      if (!eventCache.get(streamName).containsKey(id)) {
        eventCache.get(streamName).put(id, object);
      }
    });
  }
}
