package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EventCacheVerticle extends AbstractVerticle {
	private final Logger logger;
	private EventBus eventBus;
	private final List<JsonObject> eventCache = new ArrayList<>();

	public EventCacheVerticle() {
		logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void start() throws Exception {
		eventBus = vertx.eventBus();

		eventBus.consumer("read.cache.events", message -> {
			logger.info("consume read.cache.events");
			message.reply(Json.encode(eventCache));
		});
		eventBus.consumer("write.cache.events", message -> {
			final JsonObject body = (JsonObject) message.body();
			logger.info("writing to cache: " + body.encodePrettily());
			eventCache.add(body);
		});
	}
}
