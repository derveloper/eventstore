package eventstore.persistence;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;


public class InMemoryEventPersistenceVerticle extends AbstractVerticle {
  private MessageConsumer<JsonObject> jsonObjectMessageConsumer;

  @Override
  public void start() throws Exception {
    final Logger logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
    final EventPersistence service = new InMemoryEventPersistence();
    jsonObjectMessageConsumer = ProxyHelper.registerService(EventPersistence.class, vertx, service, "event-persistence");
    logger.info("deployed");
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    ProxyHelper.unregisterService(jsonObjectMessageConsumer);
  }
}
