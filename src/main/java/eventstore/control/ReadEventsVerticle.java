package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ReadEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();
		eventBus.consumer("read.events", message -> {
			logger.debug("consume read.events");
			eventBus.send("read.cache.events", message.body(), messageAsyncResult -> {
				logger.debug("reply from read.cache.events");
				message.reply(messageAsyncResult.result().body());
			});
		});
	}
}
