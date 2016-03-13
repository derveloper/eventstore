package eventstore.persistence;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import eventstore.shared.AbstractEventPersistenceVerticle;
import eventstore.shared.constants.MessageFields;
import eventstore.shared.constants.Messages;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;

import static eventstore.shared.constants.MessageFields.*;
import static eventstore.shared.constants.Messages.NOT_FOUND_MESSAGE;

public class RethinkDBEventPersistenceVerticle extends AbstractEventPersistenceVerticle {
	private static final RethinkDB r = RethinkDB.r;
	private static final String DBHOST = System.getenv("EVENTSTORE_RETHINKDB_ADDRESS") == null
			? "localhost"
			: System.getenv("EVENTSTORE_RETHINKDB_ADDRESS");

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

			vertx.executeBlocking(future -> {
				Connection conn = null;

				try {
					conn = r.connection().hostname(DBHOST).connect();
					logger.debug("fetching with query " + body.encode());
					final MapObject mapObject = RethinkUtils.getMapObjectFromJson(body);
					final List<HashMap<String, Object>> items = r.db("eventstore").table("events")
							.filter(mapObject)
							.orderBy(EVENT_CREATED_AT_FIELD)
							.run(conn);

					if (items.isEmpty()) {
						message.fail(404, new JsonObject().put(ERROR_FIELD, NOT_FOUND_MESSAGE).encodePrettily());
					} else {
						final JsonArray jsonArray = new JsonArray();
						items.forEach(hashMap -> {
							final JsonObject value = new JsonObject();
							hashMap.forEach((o, o2) -> value.put((String) o, o2));
							final JsonObject origData = value.getJsonObject(EVENT_DATA_FIELD).getJsonObject("map");
							value.remove(EVENT_DATA_FIELD);
							value.put(EVENT_DATA_FIELD, origData);
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
					message.fail(500, new JsonObject().put(ERROR_FIELD, res.cause().getMessage()).encodePrettily());
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
					persist(jsonObject, collectionName, finalConn);
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
						new JsonObject().put(ERROR_FIELD, ar.cause().getMessage()));
			}
		});
	}

	private void persist(final JsonObject body, final String collectionName, Connection finalConn) {
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
			final String streamName = body.getString(EVENT_STREAM_NAME_FIELD);
			final String eventType = body.getString(EVENT_TYPE_FIELD);
			eventBus.publish(String.format("/stream/%s?eventType=%s", streamName, eventType), body);
			logger.debug("wrote " + body.encode() + " to DB.");
		} catch (final Exception e) {
			logger.error("failed writing to db: ", e);
			eventBus.send("write.store.events.failed",
					new JsonObject().put(ERROR_FIELD, e.getMessage()));
		} finally {
			if (finalConn != null && finalConn.isOpen() && reconnected) {
				finalConn.close();
			}
		}
	}
}
