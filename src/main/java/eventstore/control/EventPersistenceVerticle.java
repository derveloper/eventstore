package eventstore.control;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
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
	private Logger logger;
	private EventBus eventBus;
	public static final RethinkDB r = RethinkDB.r;
	public static final String DBHOST = "172.17.0.2";

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();

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
					List<HashMap<String, Object>> items = r.db("eventstore").table("events")
							.filter(row -> {
								body.forEach(e -> row.g(e.getKey()).eq(e.getValue()));
								return row;
							})
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
				} catch (Exception e) {
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
					final String id = jsonObject.getString("id");
					final String collectionName = "events";
					Cursor items = r.db("eventstore").table(collectionName)
							.filter(row -> row.g("id").eq(id))
							.limit(1)
							.run(finalConn);
					if (items.toList().isEmpty()) {
						saveToMongo(jsonObject, collectionName, finalConn);
					} else {
						eventBus.send("write.store.events.duplicated",
								new JsonObject().put("message", "duplicated event id: " + id));
					}
				});
				future.complete();
			} catch (Exception e) {
				logger.error("read.idempotent.store.events.failed: ", e);
				future.fail(e);
			} finally {
				if (conn != null && conn.isOpen()) conn.close();
			}
		}, ar -> {
			if (ar.failed()) {
				eventBus.send("read.idempotent.store.events.failed",
						new JsonObject().put("error", ar.cause().getMessage()));
			}
		});
	}

	private void saveToMongo(final JsonObject body, final String collectionName, Connection finalConn) {
		logger.debug("writing to db: " + body.encodePrettily());

		try {
			final MapObject mapObject = r.hashMap();
			body.forEach(o -> mapObject.with(o.getKey(), o.getValue()));

			r.db("eventstore").table(collectionName).insert(mapObject).run(finalConn);
		} catch (Exception e) {
			logger.error("failed writing to db: ", e);
			eventBus.send("write.store.events.failed",
					new JsonObject().put("error", e.getMessage()));
		}
	}
}
