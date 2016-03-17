package eventstore.cache;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;


public class EventCacheVerticle extends AbstractVerticle {
  private MessageConsumer<JsonObject> jsonObjectMessageConsumer;

  @Override
  public void start() throws Exception {
    final Logger logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
    final EventCache service = new EventCacheImpl();
    jsonObjectMessageConsumer = ProxyHelper.registerService(EventCache.class, vertx, service, "event-cache");
    logger.info("deployed");
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    ProxyHelper.unregisterService(jsonObjectMessageConsumer);
  }
}
