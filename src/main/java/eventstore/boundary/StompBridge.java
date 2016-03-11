package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.stomp.Destination;
import io.vertx.ext.stomp.DestinationFactory;
import io.vertx.ext.stomp.StompServer;
import io.vertx.ext.stomp.StompServerHandler;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

public class StompBridge extends AbstractVerticle {
	private Logger logger;

	private static Map<String, Object> splitQuery(final URI url) throws UnsupportedEncodingException {
		final Map<String, Object> query_pairs = new LinkedHashMap<>();
		final String query = url.getQuery();
		final String[] pairs = query.split("&");
		for (final String pair : pairs) {
			final int idx = pair.indexOf("=");
			query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
		}
		return query_pairs;
	}

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		final EventBus eventBus = vertx.eventBus();
		final Integer localPort = config().getInteger("stomp.port");
		final StompServerHandler stompServerHandler = StompServerHandler.create(vertx);
		final StompServer stompServer = StompServer.create(vertx)
				.handler(stompServerHandler
						.unsubscribeHandler(serverFrame -> {
							System.out.println(serverFrame.frame());
						})
						.abortHandler(serverFrame -> {
							System.out.println(serverFrame.frame());
						})
						.closeHandler(stompServerConnection -> {
							System.out.println(stompServerConnection.session());
						})
						.subscribeHandler(serverFrame -> {
							String destination = serverFrame.frame().getDestination();
							try {
								final URI uri = new URI(destination.trim());
								final JsonObject query = uri.toString().contains("?") ? new JsonObject(splitQuery(uri)) : new JsonObject();
								final String[] split = uri.getPath().split("/");

								if (split.length != 3) {
									throw new URISyntaxException(uri.getPath(), "no stream specified");
								}

								query.put("streamName", split[2]);
								query.put("address", destination.trim());
								query.put("clientId", serverFrame.connection().session());

								logger.debug(String.format("subscribing: %s", query.encodePrettily()));
								eventBus.send("event.subscribe", query);
								stompServerHandler.destinationFactory((v, s) -> Destination.topic(vertx, destination.trim()));
							} catch (UnsupportedEncodingException | URISyntaxException e) {
								logger.error("invalid URI format", e);
							}
						})
				)
				.listen(localPort, "0.0.0.0");
		logger.info(String.format("STOMP listening on %d", stompServer.actualPort()));
	}
}
