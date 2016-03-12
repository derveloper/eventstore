package eventstore.control;

import com.rethinkdb.net.Connection;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.constants.Addresses.PERSIST_EVENTS_ADDRESS;
import static eventstore.constants.Addresses.READ_PERSISTED_EVENTS_ADDRESS;

abstract class AbstractEventPersistenceVerticle extends AbstractVerticle {
    Logger logger;
    EventBus eventBus;

    public void start() throws Exception {
        logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
        eventBus = vertx.eventBus();

        eventBus.consumer(READ_PERSISTED_EVENTS_ADDRESS, readPersistedEventsConsumer());
        eventBus.consumer(PERSIST_EVENTS_ADDRESS, writeStoreEventsConsumer());

        logger.info(String.format("Started verticle %s", this.getClass().getName()));
    }

    protected abstract Handler<Message<Object>> writeStoreEventsConsumer();

    protected abstract Handler<Message<Object>> readPersistedEventsConsumer();

    protected abstract void saveEventIfNotDuplicated(JsonArray body);

    protected abstract void persist(JsonObject body, String collectionName, Connection finalConn);
}
