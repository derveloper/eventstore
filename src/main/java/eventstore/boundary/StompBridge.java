package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.stomp.Destination;
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

	private static Map<String, Object> splitQuery(URI url) throws UnsupportedEncodingException {
		Map<String, Object> query_pairs = new LinkedHashMap<>();
		String query = url.getQuery();
		String[] pairs = query.split("&");
		for (String pair : pairs) {
			int idx = pair.indexOf("=");
			query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
		}
		return query_pairs;
	}

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final EventBus eventBus = vertx.eventBus();
		final Integer localPort = config().getInteger("stomp.port");
		final StompServer stompServer = StompServer.create(vertx)
				.handler(StompServerHandler.create(vertx)
						.destinationFactory((v, s) -> {
							try {
								final URI uri = new URI(s.trim());
								final JsonObject query = uri.toString().contains("?") ? new JsonObject(splitQuery(uri)) : new JsonObject();
								final String[] split = uri.getPath().split("/");
								if (split.length != 3) throw new URISyntaxException(uri.getPath(), "no stream specified");
								query.put("streamName", split[2]);
								query.put("address", s.trim());
								logger.debug("subscribing: " + query.encodePrettily());
								eventBus.send("event.subscribe", query);
								return Destination.topic(vertx, s.trim());
							} catch (UnsupportedEncodingException | URISyntaxException e) {
								logger.error("invalid URI format", e);
								return null;
							}
						})
				)
				.listen(localPort, "0.0.0.0");
		logger.info("STOMP listening on " + stompServer.actualPort());
	}
}
