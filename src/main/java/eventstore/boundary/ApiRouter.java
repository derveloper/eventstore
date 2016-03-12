package eventstore.boundary;

import eventstore.constants.Addresses;
import eventstore.entity.PersistedEvent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Map;

import static eventstore.constants.Addresses.READ_EVENTS_ADDRESS;
import static eventstore.constants.Addresses.WRITE_EVENTS_ADDRESS;

public class ApiRouter extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		eventBus = vertx.eventBus();
		final HttpServer httpServer = vertx.createHttpServer();
		final Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());

		router.post("/stream/:streamName*").handler(sendMessage(WRITE_EVENTS_ADDRESS, false));
		router.get("/stream/:streamName*").handler(sendMessage(READ_EVENTS_ADDRESS, true));

		listen(httpServer, router);
	}

	private Handler<RoutingContext> sendMessage(final String address, final boolean respondWithReply) {
		return routingContext -> {
			routingContext.response().putHeader("content-type", "application/json");

			if (respondWithReply) {
				final JsonObject requestBody;
				if (routingContext.getBody().length() > 0) {
					requestBody = routingContext.getBodyAsJson();
				} else {
					requestBody = new JsonObject();
				}

				for (final Map.Entry<String, String> entry : routingContext.request().params()) {
					requestBody.put(entry.getKey(), entry.getValue());
				}

				final String streamName = routingContext.request().getParam("streamName");
				requestBody.put("streamName", streamName);
				eventBus.send(address, requestBody, reply -> {
					if (reply.succeeded()) {
						final Object body = reply.result().body();
						final String responseBody;

						if (body instanceof JsonArray) {
							responseBody = ((JsonArray) body).encodePrettily();
						} else if (body instanceof JsonObject) {
							responseBody = ((JsonObject) body).encodePrettily();
						} else {
							responseBody = (String) body;
						}

						logger.debug(String.format("http response: %s", responseBody));

						routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end(responseBody);
					} else {
						@SuppressWarnings("ThrowableResultOfMethodCallIgnored") final ReplyException cause = (ReplyException) reply.cause();
						logger.warn(String.format("http respondWithReply failed: %s", cause.getMessage()));

						routingContext.response().setStatusCode(cause.failureCode()).end();
					}
				});
			} else {
				final String bodyAsString = routingContext.getBodyAsString();
				final JsonArray events = new JsonArray();
				final String streamName = routingContext.request().getParam("streamName");
				if (bodyAsString.trim().startsWith("[")) {
					routingContext.getBodyAsJsonArray().forEach(o -> {
						final JsonObject jsonObject = (JsonObject) o;
						final PersistedEvent persistedEvent = new PersistedEvent(
								jsonObject.getString("id"),
								streamName,
								jsonObject.getString("eventType", "undefined"),
								jsonObject.getJsonObject("data", new JsonObject()));
						events.add(new JsonObject(Json.encode(persistedEvent)));
					});
				} else {
					final PersistedEvent persistedEvent = new PersistedEvent(
							routingContext.getBodyAsJson().getString("id"),
							streamName,
							routingContext.getBodyAsJson().getString("eventType", "undefined"),
							routingContext.getBodyAsJson().getJsonObject("data", new JsonObject()));
					events.add(new JsonObject(Json.encode(persistedEvent)));
				}

				eventBus.send(address, events);

				final int statusCode = HttpMethod.POST.equals(routingContext.request().method())
						? HttpResponseStatus.CREATED.code()
						: HttpResponseStatus.NO_CONTENT.code();

				final String responseBody = events.encodePrettily();
				logger.debug(String.format("http optimistic response: %s", responseBody));
				routingContext.response().setStatusCode(statusCode).end(responseBody);
			}
		};
	}

	private void listen(final HttpServer httpServer, final Router router) {
		final Integer httpPort = Integer.valueOf(System.getProperty("EVENTSTORE_HTTP_PORT", "8090"));
		final Integer localPort = config().getInteger("http.port");

		logger.info(String.format("Listening on %d", localPort == null ? httpPort : localPort));

		httpServer.requestHandler(router::accept).listen((localPort == null ? httpPort : localPort));
	}
}
