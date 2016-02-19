package eventstore.control;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

import java.util.*;

public class EventPersistenceVerticle extends AbstractVerticle {
	private Logger logger;
	private EventBus eventBus;
	private final Map<String, List<JsonObject>> subscriptions = new HashMap<>();
	private StompClientConnection stompClientConnection;
	public static final RethinkDB r = RethinkDB.r;
	public static final String DBHOST = "172.17.0.2";

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
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
		logger.info("Started verticle " + this.getClass().getName());
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

			vertx.executeBlocking(future -> {
				Connection conn = null;

				try {
					conn = r.connection().hostname(DBHOST).connect();

					r.db("eventstore").tableCreate(streamName).run(conn);

					List<HashMap> items = r.db("eventstore").table("events_"+streamName)
							.filter(row -> {
								body.forEach(e -> row.g(e.getKey()).eq(e.getValue()));
								return row;
							})
							.orderBy("createdAt")
							.run(conn);

					if(items.isEmpty()) {
						message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
					}
					else {
						final JsonArray jsonArray = new JsonArray();
						items.forEach(hashMap -> {
							final JsonObject value = new JsonObject();
							hashMap.forEach((o, o2) -> value.put((String)o, o2));
							final JsonObject origData = value.getJsonObject("data").getJsonObject("map");
							value.remove("data");
							value.put("data", origData);
							jsonArray.add(value);
						});
						future.complete(jsonArray);
					}
				}
				catch (Exception e) {
					logger.error("failed reading from db: " + e);
					future.fail(e);
				}
				finally {
					if(conn != null) {
						conn.close();
					}
				}
			}, res -> {
				if(res.succeeded()) {
					message.reply(res.result());
				}
				else {
					//noinspection ThrowableResultOfMethodCallIgnored
					message.fail(500, new JsonObject().put("error", res.cause().getMessage()).encodePrettily());
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
			Connection conn = null;

			try {
				conn = r.connection().hostname(DBHOST).connect();
				r.db("eventstore").tableCreate(collectionName).run(conn);

				Cursor items = r.db("eventstore").table(collectionName)
						.filter(row -> row.g("id").eq(id))
						.limit(1)
						.run(conn);
				if(items.toList().isEmpty()) {
					saveToMongo(jsonObject, collectionName, streamName);
				}
				else {
					eventBus.send("write.store.events.duplicated",
							new JsonObject().put("message", "duplicated event id: " + id));
				}
			}
			catch (Exception e) {
				logger.error("read.idempotent.store.events.failed: ", e);
				eventBus.send("read.idempotent.store.events.failed",
						new JsonObject().put("error", e.getMessage()));
			}
			finally {
				if(conn != null) {
					conn.close();
				}
			}
		});
	}

	private void saveToMongo(final JsonObject body, final String collectionName, final String streamName) {
		logger.debug("writing to db: " + body.encodePrettily());
		vertx.executeBlocking(future -> {
			// Call some blocking API that takes a significant amount of time to return
			Connection conn = null;

			try {
				conn = r.connection().hostname(DBHOST).connect();

				final MapObject mapObject = r.hashMap();
				body.forEach(o -> mapObject.with(o.getKey(), o.getValue()));
				r.db("eventstore").table(collectionName).insert(mapObject).run(conn);
				future.complete();
			}
			catch (Exception e) {
				//noinspection ThrowableResultOfMethodCallIgnored,ThrowableResultOfMethodCallIgnored
				logger.error("failed writing to db: ", e);
				future.fail(e);
			}
			finally {
				if(conn != null) {
					conn.close();
				}
			}
		}, res -> {
			if(res.succeeded()) {
				if(subscriptions.containsKey(streamName)) {
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
			else {
				//noinspection ThrowableResultOfMethodCallIgnored
				eventBus.send("write.store.events.failed",
						new JsonObject().put("error", res.cause().getMessage()));
			}
		});
	}
}
