package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class WriteEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();
		eventBus.consumer("write.events", message -> {
			logger.debug("consume write.events " + ((JsonObject)message.body()).encodePrettily());
			eventBus.publish("write.store.events", message.body());
		});
	}
}
