package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.stomp.StompServer;
import io.vertx.ext.stomp.StompServerHandler;

import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import static eventstore.shared.constants.Addresses.EVENT_SUBSCRIBE_ADDRESS;
import static eventstore.shared.constants.Addresses.EVENT_UNSUBSCRIBE_ADDRESS;

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
		final Integer localPort = config().getInteger("stomp.port", 8091);
		final StompServerHandler stompServerHandler = StompServerHandler.create(vertx);
		final String hostAddress = Inet4Address.getLocalHost().getHostAddress();
		final StompServer stompServer = StompServer.create(vertx)
				.handler(stompServerHandler
						.closeHandler(stompServerConnection -> eventBus.send(EVENT_UNSUBSCRIBE_ADDRESS, stompServerConnection.session()))
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
								eventBus.send(EVENT_SUBSCRIBE_ADDRESS, query);
								stompServerHandler.getOrCreateDestination(destination.trim())
										.subscribe(serverFrame.connection(), serverFrame.frame());
							} catch (UnsupportedEncodingException | URISyntaxException e) {
								logger.error("invalid URI format", e);
							}
						})
				)
				.listen(localPort, hostAddress);

		final SharedData sd = vertx.sharedData();

		sd.<String, String>getClusterWideMap("eventstore-config", res -> {
			if (res.succeeded()) {
				final AsyncMap<String, String> map = res.result();
				map.put("stomp-bridge-address", hostAddress, resPut -> {
					if(resPut.succeeded()) {
						logger.info(String.format("putted address %s", hostAddress));
					}
					else {
						logger.error("failed setting bridge address");
					}
				});
			} else {
				logger.error("failed setting bridge address");
			}
		});
		logger.info(String.format("STOMP listening on %s:%d", hostAddress, stompServer.actualPort()));
	}
}
