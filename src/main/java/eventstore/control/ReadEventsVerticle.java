package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ReadEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private final Logger logger;

	public ReadEventsVerticle() {
		logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void start() throws Exception {
		eventBus = vertx.eventBus();
		eventBus.consumer("read.events", message -> {
			logger.info("consume read.events");
			eventBus.send("read.cache.events", message.body(), messageAsyncResult -> {
				logger.info("reply from read.cache.events");
				message.reply(messageAsyncResult.result().body());
			});
		});
	}
}
