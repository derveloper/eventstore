package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class WriteEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		eventBus = vertx.eventBus();
		eventBus.consumer("write.events", message -> {
			logger.debug(String.format("consume write.events %s", ((JsonArray) message.body()).encodePrettily()));
			eventBus.publish("cache.events", message.body());
			eventBus.send("persist.events", message.body());
		});
	}
}
