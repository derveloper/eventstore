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

public class InMemoryEventPersistenceVerticle extends AbstractEventPersistenceVerticle {
	private Logger logger;
	private EventBus eventBus;
	private List<JsonObject> store = new LinkedList<>();

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();

		eventBus.consumer("read.persisted.events", readPersistedEventsConsumer());
		eventBus.consumer("write.store.events", writeStoreEventsConsumer());

		logger.info("Started verticle " + this.getClass().getName());
	}

	@Override
	protected Handler<Message<Object>> writeStoreEventsConsumer() {
		return message -> {
			final JsonArray body = (JsonArray) message.body();
			saveEventIfNotDuplicated(body);
		};
	}

	@Override
	protected Handler<Message<Object>> readPersistedEventsConsumer() {
		return message -> {
			final JsonObject body = (JsonObject) message.body();
			logger.debug("consume read.persisted.events: " + body.encodePrettily());

			message.reply(new JsonArray(Json.encode(store)));
		};
	}

	@Override
	protected void saveEventIfNotDuplicated(final JsonArray body) {
		//noinspection unchecked
		store.addAll(body.getList());
	}

	@Override
	protected void persist(JsonObject body, String collectionName, Connection finalConn) { }
}
