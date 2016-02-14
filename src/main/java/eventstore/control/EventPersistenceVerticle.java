package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;

import java.util.LinkedList;
import java.util.List;

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

		eventBus.consumer("read.persisted.events", message -> {
			logger.debug("consume read.persisted.events");
			mongoClient.find("events", new JsonObject(), listAsyncResult -> {
				if(listAsyncResult.succeeded()) {
					message.reply(listAsyncResult.result());
				}
				else {
					logger.error("failed reading from db: " + listAsyncResult.cause().getMessage());
					message.fail(-1, listAsyncResult.cause().getMessage());
				}
			});
		});
		eventBus.consumer("write.store.events", message -> {
			final JsonObject body = (JsonObject) message.body();
			final String id = body.getString("id");
			mongoClient.find("events", new JsonObject().put("id", id), findResult -> {
				if(findResult.succeeded() && findResult.result().isEmpty()) {
					logger.debug("writing to db: " + body.encodePrettily());
					mongoClient.save("events", body, saveResult -> {
						if(saveResult.failed()) {
							logger.error("failed writing to db: " + saveResult.cause().getMessage());
							eventBus.send("write.store.events.failed",
									new JsonObject().put("error", saveResult.cause().getMessage()));
						}
					});
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
}
