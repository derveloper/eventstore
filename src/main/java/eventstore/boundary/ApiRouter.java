package eventstore.boundary;

import eventstore.entity.PersistedEvent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ApiRouter extends AbstractVerticle {
	private EventBus eventBus;
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		eventBus = vertx.eventBus();
		final HttpServer httpServer = vertx.createHttpServer();
		final Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());

		router.post("/stream/*").handler(sendMessage("write.events", false));
		router.get("/stream/*").handler(sendMessage("read.events", true));

		listen(httpServer, router);
	}

	private Handler<RoutingContext> sendMessage(final String address, final boolean respondWithReply) {
		return routingContext -> {
			routingContext.response().putHeader("content-type", "application/json");
			final JsonObject requestBody;
			if (routingContext.getBody().length() > 0) {
				requestBody = routingContext.getBodyAsJson();
			} else {
				requestBody = new JsonObject();
			}

			for (Map.Entry<String, String> entry : routingContext.request().params()) {
				requestBody.put(entry.getKey(), entry.getValue());
			}

			if (respondWithReply) {
				eventBus.send(address, requestBody, reply -> {
					if (reply.succeeded()) {
						final Object body = reply.result().body();
						final String responseBody;

						if(body instanceof JsonArray) {
							responseBody = ((JsonArray)body).encodePrettily();
						}
						else if(body instanceof JsonObject) {
							responseBody = ((JsonObject)body).encodePrettily();
						}
						else {
							responseBody = (String)body;
						}

						logger.debug("http response: " + responseBody);

						routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end(responseBody);
					}
					else {
						logger.warn("http respondWithReply failed: " + reply.cause().getMessage());

						routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
					}
				});
			} else {
				final PersistedEvent event = new PersistedEvent(
						requestBody.getString("eventType", "undefined"),
						requestBody.getJsonObject("data", new JsonObject()));
				final int statusCode = HttpMethod.POST.equals(routingContext.request().method())
						? HttpResponseStatus.CREATED.code()
						: HttpResponseStatus.NO_CONTENT.code();
				final String responseBody = Json.encode(event);
				logger.debug("http optimistic response: " + responseBody);
				routingContext.response().setStatusCode(statusCode).end(responseBody);
				eventBus.publish(address, new JsonObject(responseBody));
			}
		};
	}

	private void listen(final HttpServer httpServer, final Router router) throws IOException {
		final Integer localPort = config().getInteger("http.port");

		logger.info("Listening on " + localPort);

		httpServer.requestHandler(router::accept).listen(localPort);
	}
}
