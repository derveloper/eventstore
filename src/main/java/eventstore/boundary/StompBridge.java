package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.stomp.BridgeOptions;
import io.vertx.ext.stomp.StompServer;
import io.vertx.ext.stomp.StompServerHandler;

import javax.print.attribute.URISyntax;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StompBridge extends AbstractVerticle {
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final EventBus eventBus = vertx.eventBus();
		final Integer localPort = config().getInteger("stomp.port");
		final StompServer stompServer = StompServer.create(vertx)
				.handler(StompServerHandler.create(vertx)
						.bridge(new BridgeOptions()
								.addInboundPermitted(new PermittedOptions().setAddressRegex("^.*$"))
								.addOutboundPermitted(new PermittedOptions().setAddressRegex("^.*$"))
						)
						.subscribeHandler(serverFrame -> {
							try {
								final URI uri = new URI(serverFrame.frame().getDestination());
								final JsonObject query = new JsonObject(splitQuery(uri));
								final String[] split = uri.getPath().split("/");
								if(split.length != 3) throw new URISyntaxException(uri.getPath(), "no stream specified");
								query.put("streamName", split[2]);
								query.put("address", uri.toString());
								logger.debug(query.encodePrettily());
								eventBus.publish("event.subscribe", query);
							} catch (UnsupportedEncodingException | URISyntaxException e) {
								logger.warn("invalid URI format", e);
								serverFrame.frame().setBody(Buffer.buffer("invalid URI format"));
								serverFrame.connection().close();
							}
						})
				)
				.listen(localPort, "0.0.0.0");
		logger.info("STOMP listening on " + stompServer.actualPort());
	}

	public static Map<String, Object> splitQuery(URI url) throws UnsupportedEncodingException {
		Map<String, Object> query_pairs = new LinkedHashMap<>();
		String query = url.getQuery();
		String[] pairs = query.split("&");
		for (String pair : pairs) {
			int idx = pair.indexOf("=");
			query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
		}
		return query_pairs;
	}
}
