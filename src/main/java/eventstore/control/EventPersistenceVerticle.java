package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EventPersistenceVerticle extends AbstractVerticle {
	private Logger logger;
	private EventBus eventBus;
	private MongoClient mongoClient;
	private final Map<String, List<JsonObject>> subscriptions = new HashMap<>();
	private StompClientConnection stompClientConnection;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final String mongodbHosts = System.getProperty("EVENTSTORE_MONGODB_HOSTS", "mongodb://127.0.0.1:27017");
		final String mongodbName = System.getProperty("EVENTSTORE_MONGODB_NAME", "eventstore");
		final JsonObject config = new JsonObject()
				.put("waitQueueMultiple", 100)
				.put("db_name", mongodbName)
				.put("connection_string", mongodbHosts);
		mongoClient = MongoClient.createShared(vertx, config);
		eventBus = vertx.eventBus();

		final Integer stompPort = config().getInteger("stomp.port");
		if (stompPort != null) {
			StompClient.create(vertx, new StompClientOptions()
					.setHost("0.0.0.0").setPort(stompPort)
			).connect(ar -> {
				if (ar.succeeded()) {
					logger.debug("connected to STOMP");
					stompClientConnection = ar.result();
				} else {
					logger.warn("could not connect to STOMP", ar.cause());
				}
			});
		}

		eventBus.consumer("read.persisted.events", readPersistedEventsConsumer());
		eventBus.consumer("write.store.events", writeStoreEventsConsumer());
		eventBus.consumer("event.subscribe", message -> {
			final JsonObject body = (JsonObject) message.body();
			final String streamName = (String) body.remove("streamName");
			if (!subscriptions.containsKey(streamName)) {
				subscriptions.put(streamName, new LinkedList<>());
			}
			subscriptions.get(streamName).add(body);
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
				if (listAsyncResult.succeeded() && !listAsyncResult.result().isEmpty()) {
					final JsonArray jsonArray = new JsonArray();
					listAsyncResult.result().forEach(jsonArray::add);
					message.reply(jsonArray);
				} else if (listAsyncResult.succeeded()) {
					message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
				} else {
					@SuppressWarnings("ThrowableResultOfMethodCallIgnored") final String failMessage = listAsyncResult.cause().getMessage();
					logger.error("failed reading from db: " + failMessage);
					message.fail(500, new JsonObject().put("error", failMessage).encodePrettily());
				}
			});
		};
	}

	private void saveEventIfNotDuplicated(final JsonArray body) {
		body.forEach(o -> {
			final JsonObject jsonObject = (JsonObject) o;
			final String id = jsonObject.getString("id");
			final String streamName = jsonObject.getString("streamName");
			jsonObject.remove("streamName");
			final String collectionName = "events_" + streamName;
			mongoClient.find(collectionName, new JsonObject().put("id", id), findResult -> {
				if (findResult.succeeded() && findResult.result().isEmpty()) {
					saveToMongo(jsonObject, collectionName, streamName);
				} else if (findResult.succeeded()) {
					eventBus.send("write.store.events.duplicated",
							new JsonObject().put("message", "duplicated event id: " + id));
				} else if (findResult.failed()) {
					//noinspection ThrowableResultOfMethodCallIgnored,ThrowableResultOfMethodCallIgnored
					eventBus.send("read.idempotent.store.events.failed",
							new JsonObject().put("error", findResult.cause().getMessage()));
				}
			});
		});
	}

	private void saveToMongo(final JsonObject body, final String collectionName, final String streamName) {
		logger.debug("writing to db: " + body.encodePrettily());
		mongoClient.save(collectionName, body, saveResult -> {
			if (saveResult.failed()) {
				//noinspection ThrowableResultOfMethodCallIgnored,ThrowableResultOfMethodCallIgnored
				logger.error("failed writing to db: " + saveResult.cause().getMessage());
				//noinspection ThrowableResultOfMethodCallIgnored
				eventBus.send("write.store.events.failed",
						new JsonObject().put("error", saveResult.cause().getMessage()));
			} else {
				if (stompClientConnection != null && subscriptions.containsKey(streamName)) {
					final List<JsonObject> jsonObjects = subscriptions.get(streamName);
					jsonObjects.forEach(entries -> {
						final String address = (String) entries.remove("address");
						entries.stream()
								.filter(stringObjectEntry -> body.containsKey(stringObjectEntry.getKey()))
								.findAny()
								.ifPresent(stringObjectEntry1 -> {
									logger.debug("publishing to: " + address);
									final Frame frame = new Frame();
									frame.setCommand(Frame.Command.SEND);
									frame.setDestination(address);
									body.remove("_id");
									frame.setBody(Buffer.buffer(body.encodePrettily()));
									stompClientConnection.send(frame);
								});
					});
				}
			}
		});
	}
}
