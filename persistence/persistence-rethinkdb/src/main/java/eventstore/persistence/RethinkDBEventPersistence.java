package eventstore.persistence;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.List;

import static eventstore.shared.constants.MessageFields.*;
import static eventstore.shared.constants.Messages.NOT_FOUND_MESSAGE;


class RethinkDBEventPersistence implements EventPersistence {
  private static final RethinkDB r = RethinkDB.r;
  private static final String DBHOST = System.getenv("EVENTSTORE_RETHINKDB_ADDRESS") == null
                                       ? "localhost"
                                       : System.getenv("EVENTSTORE_RETHINKDB_ADDRESS");
  private final Logger logger;
  private final Vertx vertx;

  RethinkDBEventPersistence(final Vertx vertx) {
    this.vertx = vertx;
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "event-persistence"));
  }

  @Override
  public void read(JsonObject query, Handler<AsyncResult<JsonArray>> result) {
    logger.debug("consume read.persisted.events: " + query.encodePrettily());

    vertx.executeBlocking(future -> {
      Connection conn = null;

      try {
        conn = r.connection().hostname(DBHOST).connect();
        logger.debug("fetching with query " + query.encode());
        final MapObject mapObject = RethinkUtils.getMapObjectFromJson(query);
        final List<HashMap<String, Object>> items = r.db("eventstore").table("events")
                                                     .filter(mapObject)
                                                     .orderBy(EVENT_CREATED_AT_FIELD)
                                                     .run(conn);

        if (items.isEmpty()) {
          result.handle(Future.failedFuture(
              new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 404, NOT_FOUND_MESSAGE)));
        }
        else {
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
      }
      catch (final Exception e) {
        logger.error("failed reading from db: " + e);
        future.fail(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 500, e.getMessage()));
      }
      finally {
        if (conn != null) {
          conn.close();
        }
      }
    }, res -> {
      if (res.succeeded()) {
        result.handle(Future.succeededFuture((JsonArray) res.result()));
      }
      else {
        result.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  @Override
  public void write(final JsonArray events, final Handler<AsyncResult<Boolean>> result) {
    saveEventIfNotDuplicated(events, result);
  }

  private void saveEventIfNotDuplicated(final JsonArray body, final Handler<AsyncResult<Boolean>> result) {
    vertx.executeBlocking(future -> {
      Connection conn = null;
      try {
        conn = r.connection().hostname(DBHOST).connect();
        final Connection finalConn = conn;
        body.forEach(o -> {
          final JsonObject jsonObject = (JsonObject) o;
          final String collectionName = "events";
          persist(jsonObject, collectionName, finalConn, future);
        });
        if (!future.isComplete() && !future.succeeded() && !future.failed()) {
          future.complete();
        }
      }
      catch (final Exception e) {
        logger.error("read.idempotent.store.events.failed: ", e);
        if (!future.isComplete() && !future.succeeded() && !future.failed()) {
          future.fail(e);
        }
      }
      finally {
        if (conn != null && conn.isOpen()) { conn.close(); }
      }
    }, ar -> {
      if (ar.failed()) {
        //noinspection ThrowableResultOfMethodCallIgnored
        result.handle(Future.failedFuture(new JsonObject().put(ERROR_FIELD, ar.cause().getMessage()).encode()));
      }
      else {
        result.handle(Future.succeededFuture());
      }
    });
  }

  private void persist(final JsonObject body, final String collectionName, Connection finalConn, Future<Object> future) {
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
    }
    catch (final Exception e) {
      logger.error("failed writing to db: ", e);
      future.fail(e);
    }
    finally {
      if (finalConn != null && finalConn.isOpen() && reconnected) {
        finalConn.close();
      }
    }
  }
}
