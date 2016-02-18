package eventstore.control;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class EventCacheVerticle extends AbstractVerticle {
	private Logger logger;
	private final Map<String, Map<String, JsonObject>> eventCache = new LinkedHashMap<>();

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final EventBus eventBus = vertx.eventBus();

		eventBus.consumer("read.cache.events", message -> {
			final JsonObject body = (JsonObject) message.body();
			final String streamName = (String) body.remove("streamName");
			logger.debug("consume read.cache.events: " + body.encodePrettily());
			if(body.containsKey("id")) {
				if(eventCache.containsKey(streamName) && eventCache.get(streamName).containsKey(body.getString("id"))) {
					message.reply(new JsonArray().add(new JsonObject(Json.encode(eventCache.get(body.getString("id"))))));
				}
				else {
					message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
				}
			}
			else if(!body.isEmpty()) {
				message.fail(404, new JsonObject().put("error", "not found").encodePrettily());
			}
			else {
				final JsonArray jsonArray = new JsonArray();
				if(eventCache.containsKey(streamName)) {
					eventCache.get(streamName).values().forEach(jsonArray::add);
				}
				message.reply(jsonArray);
			}
		});
		eventBus.consumer("write.store.events", this::writeCache);
		eventBus.consumer("write.cache.events", this::writeCache);
	}

	private void writeCache(final Message<Object> message) {
		logger.debug("writing to cache");
		final Object body1 = message.body();
		if(body1 instanceof JsonArray) {
			((JsonArray)body1).forEach(o -> {
				final JsonObject object = (JsonObject) o;
				final String streamName = (String) object.remove("streamName");
				final String id = object.getString("id");
				if(!eventCache.containsKey(streamName)) {
					eventCache.put(streamName, new LinkedHashMap<>());
				}
				if(!eventCache.get(streamName).containsKey(id)) {
					eventCache.get(streamName).put(id, object);
				}
			});
		}
		else {
			final JsonObject body = (JsonObject) body1;
			final String id = body.getString("id");
			final String streamName = (String) body.remove("streamName");
			if(!eventCache.containsKey(streamName)) {
				eventCache.put(streamName, new LinkedHashMap<>());
			}
			if(!eventCache.containsKey(id)) {
				eventCache.get(streamName).put(id, body);
			}
		}
	}
}
