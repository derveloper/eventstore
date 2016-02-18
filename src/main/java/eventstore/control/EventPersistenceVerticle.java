package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

import java.util.Collections;

public class EventPersistenceVerticle extends AbstractVerticle {
	private Logger logger;
	private EventBus eventBus;

	private MongoClient mongoClient;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final JsonObject config = new JsonObject()
				.put("db_name", "eventstore")
				.put("connection_string", "mongodb://127.0.0.1:27017");
		mongoClient = MongoClient.createShared(vertx, config);
		eventBus = vertx.eventBus();

		eventBus.consumer("read.persisted.events", readPersistedEventsConsumer());
		eventBus.consumer("write.store.events", writeStoreEventsConsumer());
		eventBus.consumer("event.subscribe", message -> {
			final JsonObject body = (JsonObject) message.body();
			final String streamName = (String) body.remove("streamName");
			final String address = (String) body.remove("address");
		});
	}

	private Handler<Message<Object>> writeStoreEventsConsumer() {
		return message -> {
			final JsonArray body = (JsonArray) message.body();
			saveEventIfNotDuplicated(body);
		};
	}

	private Handler<Message<Object>> readPersistedEventsConsumer() {
		return message -> {
			final JsonObject body = (JsonObject) message.body();
			final String streamName = body.getString("streamName");
			body.remove("streamName");
			logger.debug("consume read.persisted.events: " + body.encodePrettily());

			mongoClient.find("events_" + streamName, body, listAsyncResult -> {
				if(listAsyncResult.succeeded() && !listAsyncResult.result().isEmpty()) {
					final JsonArray jsonArray = new JsonArray();
					listAsyncResult.result().forEach(jsonArray::add);
					message.reply(jsonArray);
				}
				else if(listAsyncResult.succeeded()) {
					message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
				}
				else {
					final String failMessage = listAsyncResult.cause().getMessage();
					logger.error("failed reading from db: " + failMessage);
					message.fail(500, new JsonObject().put("error", failMessage).encodePrettily());
				}
			});
		};
	}

	private void saveEventIfNotDuplicated(JsonArray body) {
		body.forEach(o -> {
			final JsonObject jsonObject = (JsonObject) o;
			final String id = jsonObject.getString("id");
			final String streamName = jsonObject.getString("streamName");
			jsonObject.remove("streamName");
			final String collectionName = "events_" + streamName;
			mongoClient.find(collectionName, new JsonObject().put("id", id), findResult -> {
				if(findResult.succeeded() && findResult.result().isEmpty()) {
					logger.debug("writing to db: " + body.encodePrettily());
					saveToMongo(jsonObject, collectionName);
				}
				else if(findResult.succeeded()) {
					eventBus.send("write.store.events.duplicated",
							new JsonObject().put("message", "duplicated event id: " + id));
				}
				else if(findResult.failed()) {
					eventBus.send("read.idempotent.store.events.failed",
							new JsonObject().put("error", findResult.cause().getMessage()));
				}
			});
		});
	}

	private void saveToMongo(JsonObject body, String collectionName) {
		mongoClient.save(collectionName, body, saveResult -> {
			if(saveResult.failed()) {
				logger.error("failed writing to db: " + saveResult.cause().getMessage());
				eventBus.send("write.store.events.failed",
						new JsonObject().put("error", saveResult.cause().getMessage()));
			}
			else {
				eventBus.send("write.store.events.persisted", body);
			}
		});
	}
}
