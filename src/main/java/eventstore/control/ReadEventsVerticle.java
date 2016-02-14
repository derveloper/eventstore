package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
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
			logger.debug("consume read.events: " + ((JsonObject)message.body()).encodePrettily());
			eventBus.send("read.cache.events", message.body(), messageAsyncResult -> {
				if(messageAsyncResult.succeeded()) {
					logger.debug("reply from read.cache.events");
					message.reply(messageAsyncResult.result().body());
				}
				else {
					eventBus.send("read.persisted.events", message.body(), dbMessageAsyncResult -> {
						if(dbMessageAsyncResult.succeeded()) {
							logger.debug("reply from read.persisted.events");
							eventBus.send("write.cache.events", dbMessageAsyncResult.result().body());
							message.reply(dbMessageAsyncResult.result().body());
						}
						else {
							logger.error("reply from read.persisted.events failed!");
							message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
						}
					});
				}
			});
		});
	}
}
