package eventstore.control;

import com.rethinkdb.net.Connection;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

abstract class AbstractEventPersistenceVerticle extends AbstractVerticle {
    protected abstract Handler<Message<Object>> writeStoreEventsConsumer();

    protected abstract Handler<Message<Object>> readPersistedEventsConsumer();

    protected abstract void saveEventIfNotDuplicated(JsonArray body);

    protected abstract void persist(JsonObject body, String collectionName, Connection finalConn);
}
