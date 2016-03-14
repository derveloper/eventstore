package eventstore.persistence;

import eventstore.shared.AbstractEventPersistenceVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.LinkedList;
import java.util.List;


public class InMemoryEventPersistenceVerticle extends AbstractEventPersistenceVerticle {
  private final List<JsonObject> store = new LinkedList<>();

  @Override
  protected Handler<Message<Object>> readPersistedEventsConsumer() {
    return message -> {
      final JsonObject body = (JsonObject) message.body();
      logger.debug(String.format("consume read.persisted.events: %s", body.encodePrettily()));

      message.reply(new JsonArray(Json.encode(store)));
    };
  }

  @Override
  protected Handler<Message<Object>> writeStoreEventsConsumer() {
    return message -> {
      final JsonArray body = (JsonArray) message.body();
      saveEventIfNotDuplicated(body);
      message.reply(true);
    };
  }

  private void saveEventIfNotDuplicated(final JsonArray body) {
    //noinspection unchecked
    store.addAll(body.getList());
    logger.debug(String.format("persisted %s", body.encodePrettily()));
    if (!body.isEmpty()) {
      final JsonObject first = body.getJsonObject(0);
      eventBus.publish(
          String.format("/stream/%s?eventType=%s", first.getString("streamName"), first.getString("eventType")), body);
    }
  }
}
