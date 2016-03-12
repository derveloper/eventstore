package eventstore.control;

import com.rethinkdb.net.Connection;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static eventstore.constants.Addresses.PERSIST_EVENTS_ADDRESS;
import static eventstore.constants.Addresses.READ_PERSISTED_EVENTS_ADDRESS;

public class InMemoryEventPersistenceVerticle extends AbstractEventPersistenceVerticle {
	private List<JsonObject> store = new LinkedList<>();

	@Override
	protected Handler<Message<Object>> writeStoreEventsConsumer() {
		return message -> {
			final JsonArray body = (JsonArray) message.body();
			saveEventIfNotDuplicated(body);
			message.reply(true);
		};
	}

	@Override
	protected Handler<Message<Object>> readPersistedEventsConsumer() {
		return message -> {
			final JsonObject body = (JsonObject) message.body();
			logger.debug(String.format("consume read.persisted.events: %s", body.encodePrettily()));

			message.reply(new JsonArray(Json.encode(store)));
		};
	}

	@Override
	protected void saveEventIfNotDuplicated(final JsonArray body) {
		//noinspection unchecked
		store.addAll(body.getList());
		logger.debug(String.format("persisted %s", body.encodePrettily()));
		if(!body.isEmpty()) {
			JsonObject first = body.getJsonObject(0);
			eventBus.publish(String.format("/stream/%s?eventType=%s", first.getString("streamName"), first.getString("eventType")), body);
		}
	}

	@Override
	protected void persist(JsonObject body, String collectionName, Connection finalConn) { }
}
