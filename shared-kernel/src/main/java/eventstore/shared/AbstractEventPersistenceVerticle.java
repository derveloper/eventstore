package eventstore.shared;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.shared.constants.Addresses.PERSIST_EVENTS_ADDRESS;
import static eventstore.shared.constants.Addresses.READ_PERSISTED_EVENTS_ADDRESS;


public abstract class AbstractEventPersistenceVerticle extends AbstractVerticle {
  protected Logger logger;
  protected EventBus eventBus;

  public void start() throws Exception {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
    eventBus = vertx.eventBus();

    eventBus.consumer(READ_PERSISTED_EVENTS_ADDRESS, readPersistedEventsConsumer());
    eventBus.consumer(PERSIST_EVENTS_ADDRESS, writeStoreEventsConsumer());

    logger.info(String.format("Started verticle %s", this.getClass().getName()));
  }

  protected abstract Handler<Message<Object>> readPersistedEventsConsumer();

  protected abstract Handler<Message<Object>> writeStoreEventsConsumer();
}
