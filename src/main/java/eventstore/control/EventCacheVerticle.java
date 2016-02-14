package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class EventCacheVerticle extends AbstractVerticle {
	private Logger logger;
	private EventBus eventBus;
	private final Map<String, JsonObject> eventCache = new HashMap<>();

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();

		eventBus.consumer("read.cache.events", message -> {
			logger.debug("consume read.cache.events");
			message.reply(Json.encode(eventCache));
		});
		eventBus.consumer("write.store.events", this::writeCache);
		eventBus.consumer("write.cache.events", this::writeCache);
	}

	private void writeCache(final Message<Object> message) {
		final JsonObject body = (JsonObject) message.body();
		logger.debug("writing to cache: " + (body).encodePrettily());
		final String id = body.getString("id");
		if(!eventCache.containsKey(id)) {
			eventCache.put(id, body);
		}
	}
}
