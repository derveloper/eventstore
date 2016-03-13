package eventstore.cache;

import eventstore.shared.constants.Addresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static eventstore.shared.constants.Addresses.CACHE_EVENTS_ADDRESS;
import static eventstore.shared.constants.MessageFields.ERROR_FIELD;
import static eventstore.shared.constants.MessageFields.EVENT_ID_FIELD;
import static eventstore.shared.constants.MessageFields.EVENT_STREAM_NAME_FIELD;
import static eventstore.shared.constants.Messages.NOT_FOUND_MESSAGE;

public class EventCacheVerticle extends AbstractVerticle {
	private final Map<String, Map<String, JsonObject>> eventCache = new LinkedHashMap<>();
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		final EventBus eventBus = vertx.eventBus();

		eventBus.consumer(Addresses.READ_CACHE_EVENTS_ADDRESS, message -> {
			final JsonObject body = (JsonObject) message.body();
			final String streamName = (String) body.remove(EVENT_STREAM_NAME_FIELD);
			logger.debug(String.format("consume read.cache.events: %s", body.encodePrettily()));
			if (body.containsKey(EVENT_ID_FIELD)) {
				if (eventCache.containsKey(streamName) && eventCache.get(streamName).containsKey(body.getString(EVENT_ID_FIELD))) {
					message.reply(new JsonArray().add(new JsonObject(Json.encode(eventCache.get(streamName).get(body.getString(EVENT_ID_FIELD))))));
				} else {
					message.fail(404, new JsonObject().put(ERROR_FIELD, NOT_FOUND_MESSAGE).encodePrettily());
				}
			} else if (!body.isEmpty()) {
				message.fail(404, new JsonObject().put(ERROR_FIELD, NOT_FOUND_MESSAGE).encodePrettily());
			} else {
				final JsonArray jsonArray = new JsonArray();
				if (eventCache.containsKey(streamName)) {
					eventCache.get(streamName).values().forEach(jsonArray::add);
				}
				message.reply(jsonArray);
			}
		});
		eventBus.consumer(CACHE_EVENTS_ADDRESS, this::writeCache);
	}

	private void writeCache(final Message<Object> message) {
		logger.debug("writing to cache");
		final Object body1 = message.body();
		if (body1 instanceof JsonArray) {
			((JsonArray) body1).forEach(o -> {
				final JsonObject object = (JsonObject) o;
				final String streamName = (String) object.remove(EVENT_STREAM_NAME_FIELD);
				final String id = object.getString(EVENT_ID_FIELD);
				eventCache.putIfAbsent(streamName, new LinkedHashMap<>());
				if (!eventCache.get(streamName).containsKey(id)) {
					eventCache.get(streamName).put(id, object);
				}
			});
		} else {
			final JsonObject body = (JsonObject) body1;
			final String id = body.getString(EVENT_ID_FIELD);
			final String streamName = (String) body.remove(EVENT_STREAM_NAME_FIELD);
			eventCache.putIfAbsent(streamName, new LinkedHashMap<>());
			if (!eventCache.containsKey(id)) {
				eventCache.get(streamName).put(id, body);
			}
		}
	}
}
