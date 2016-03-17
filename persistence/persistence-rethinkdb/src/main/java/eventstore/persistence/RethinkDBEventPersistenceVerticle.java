package eventstore.persistence;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;


public class RethinkDBEventPersistenceVerticle extends AbstractVerticle {
  private MessageConsumer<JsonObject> jsonObjectMessageConsumer;

  public void start() throws Exception {
    Logger logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
    final EventPersistence service = new RethinkDBEventPersistence(vertx);
    jsonObjectMessageConsumer = ProxyHelper.registerService(EventPersistence.class, vertx, service, "event-persistence");

    logger.info(String.format("Started verticle %s", this.getClass().getName()));
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    ProxyHelper.unregisterService(jsonObjectMessageConsumer);
  }
}
