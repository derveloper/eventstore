package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class EventPersistenceVerticle extends AbstractVerticle {
	private Logger logger;
	private EventBus eventBus;
	private final List<JsonObject> eventCache = new LinkedList<>();

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();

		eventBus.consumer("read.persisted.events", message -> {
			logger.debug("consume read.persisted.events");
			message.reply(Json.encode(eventCache));
		});
		eventBus.consumer("write.store.events", message -> {
			logger.debug("writing to db: " + ((JsonObject)message.body()).encodePrettily());
			eventCache.add((JsonObject)message.body());
		});
	}
}
