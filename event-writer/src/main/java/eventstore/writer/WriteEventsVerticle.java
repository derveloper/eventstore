package eventstore.writer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;

import static eventstore.shared.constants.Addresses.*;


public class WriteEventsVerticle extends AbstractVerticle {
  private MessageConsumer<JsonObject> jsonObjectMessageConsumer;

  @Override
  public void start() throws Exception {
    final Logger logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
    final EventBus eventBus = vertx.eventBus();
    final EventWriter service = new EventWriterImpl(eventBus);
    jsonObjectMessageConsumer = ProxyHelper.registerService(EventWriter.class, vertx, service, "event-writer");
    logger.info("deployed");
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    ProxyHelper.unregisterService(jsonObjectMessageConsumer);
  }
}
