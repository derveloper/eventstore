package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static eventstore.constants.Addresses.READ_CACHE_EVENTS_ADDRESS;
import static eventstore.constants.Addresses.READ_EVENTS_ADDRESS;

public class ReadEventsVerticle extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		eventBus = vertx.eventBus();
		eventBus.consumer(READ_EVENTS_ADDRESS, message -> {
			logger.debug(String.format("consume read.events: %s", ((JsonObject) message.body()).encodePrettily()));
			final DeliveryOptions cacheDeliveryOptions = new DeliveryOptions()
					.setSendTimeout(200);
			eventBus.send(READ_CACHE_EVENTS_ADDRESS, message.body(), cacheDeliveryOptions, messageAsyncResult -> {
				if (messageAsyncResult.succeeded()) {
					logger.debug("reply from read.cache.events");
					message.reply(messageAsyncResult.result().body());
				} else {
					logger.debug("reply from read.cache.events not successful, getting event from db");
					eventBus.send("read.persisted.events", message.body(), dbMessageAsyncResult -> {
						if (dbMessageAsyncResult.succeeded()) {
							logger.debug("reply from read.persisted.events");
							eventBus.send("write.cache.events", dbMessageAsyncResult.result().body());
							message.reply(dbMessageAsyncResult.result().body());
						} else {
							logger.error("reply from read.persisted.events failed!");
							message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
						}
					});
				}
			});
		});
	}
}
