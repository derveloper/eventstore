package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class WriteEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private final Logger logger;

	public WriteEventsVerticle() {
		logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void start() throws Exception {
		eventBus = vertx.eventBus();
		eventBus.consumer("write.events", message -> {
			logger.info("consume write.events {}", (String)message.body());
			eventBus.send("write.cache.events", message.body());
		});
	}
}
