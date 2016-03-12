package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.constants.Addresses.*;

public class WriteEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		eventBus = vertx.eventBus();
		eventBus.consumer(WRITE_EVENTS_ADDRESS, message -> {
			logger.debug(String.format("consume write.events %s", ((JsonArray) message.body()).encodePrettily()));
			eventBus.send(PERSIST_EVENTS_ADDRESS, message.body(), messageAsyncResult -> {
				if(messageAsyncResult.succeeded()) {
					eventBus.publish(CACHE_EVENTS_ADDRESS, message.body());
				}
			});
		});
	}
}
