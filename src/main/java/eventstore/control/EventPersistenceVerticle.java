package eventstore.control;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import eventstore.util.RethinkUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class EventPersistenceVerticle extends AbstractVerticle {
	private static final RethinkDB r = RethinkDB.r;
	private static final String DBHOST = System.getenv("EVENTSTORE_RETHINKDB_ADDRESS") == null ? "localhost" : System.getenv("EVENTSTORE_RETHINKDB_ADDRESS");
	private Logger logger;
	private EventBus eventBus;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();

		logger.info("connecting to rethink on " + DBHOST);

		eventBus.consumer("read.persisted.events", readPersistedEventsConsumer());
		eventBus.consumer("write.store.events", writeStoreEventsConsumer());

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
			logger.debug("consume read.persisted.events: " + body.encodePrettily());

			vertx.executeBlocking(future -> {
				Connection conn = null;

				try {
					conn = r.connection().hostname(DBHOST).connect();
					logger.debug("fetching with query " + body.encode());
					final MapObject mapObject = RethinkUtils.getMapObjectFromJson(body);
					final List<HashMap<String, Object>> items = r.db("eventstore").table("events")
							.filter(mapObject)
							.orderBy("createdAt")
							.run(conn);

					if (items.isEmpty()) {
						message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
					} else {
						final JsonArray jsonArray = new JsonArray();
						items.forEach(hashMap -> {
							final JsonObject value = new JsonObject();
							hashMap.forEach((o, o2) -> value.put((String) o, o2));
							final JsonObject origData = value.getJsonObject("data").getJsonObject("map");
							value.remove("data");
							value.put("data", origData);
							jsonArray.add(value);
						});
						future.complete(jsonArray);
					}
				} catch (final Exception e) {
					logger.error("failed reading from db: " + e);
					future.fail(e);
				} finally {
					if (conn != null) {
						conn.close();
					}
				}
			}, res -> {
				if (res.succeeded()) {
					message.reply(res.result());
				} else {
					//noinspection ThrowableResultOfMethodCallIgnored
					message.fail(500, new JsonObject().put("error", res.cause().getMessage()).encodePrettily());
				}
			});
		};
	}

	private void saveEventIfNotDuplicated(final JsonArray body) {
		vertx.executeBlocking(future -> {
			Connection conn = null;
			try {
				conn = r.connection().hostname(DBHOST).connect();
				final Connection finalConn = conn;
				body.forEach(o -> {
					final JsonObject jsonObject = (JsonObject) o;
					final String collectionName = "events";
					saveToMongo(jsonObject, collectionName, finalConn);
				});
				if (!future.isComplete() && !future.succeeded() && !future.failed()) {
					future.complete();
				}
			} catch (final Exception e) {
				logger.error("read.idempotent.store.events.failed: ", e);
				if (!future.isComplete() && !future.succeeded() && !future.failed()) {
					future.fail(e);
				}
			} finally {
				if (conn != null && conn.isOpen()) conn.close();
			}
		}, ar -> {
			if (ar.failed()) {
				//noinspection ThrowableResultOfMethodCallIgnored
				eventBus.send("read.idempotent.store.events.failed",
						new JsonObject().put("error", ar.cause().getMessage()));
			}
		});
	}

	private void saveToMongo(final JsonObject body, final String collectionName, Connection finalConn) {
		logger.debug("writing to db: " + body.encodePrettily());

		boolean reconnected = false;
		try {
			if (!finalConn.isOpen()) {
				logger.debug("lost connection, reopening");
				finalConn = r.connection().hostname(DBHOST).connect();
				reconnected = true;
			}
			final MapObject mapObject = RethinkUtils.getMapObjectFromJson(body);
			r.db("eventstore").table(collectionName).insert(mapObject).run(finalConn);
			logger.debug("wrote " + body.encode() + " to DB.");
		} catch (final Exception e) {
			logger.error("failed writing to db: ", e);
			eventBus.send("write.store.events.failed",
					new JsonObject().put("error", e.getMessage()));
		} finally {
			if (finalConn != null && finalConn.isOpen() && reconnected) {
				finalConn.close();
			}
		}
	}
}
