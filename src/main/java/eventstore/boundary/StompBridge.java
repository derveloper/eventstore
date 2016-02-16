package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.stomp.BridgeOptions;
import io.vertx.ext.stomp.StompServer;
import io.vertx.ext.stomp.StompServerHandler;

public class StompBridge extends AbstractVerticle {
	private Logger logger;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final Integer localPort = config().getInteger("stomp.port");
		final StompServer stompServer = StompServer.create(vertx)
				.handler(StompServerHandler.create(vertx)
						.bridge(new BridgeOptions()
								.addInboundPermitted(new PermittedOptions().setAddressRegex("^.*$"))
								.addOutboundPermitted(new PermittedOptions().setAddressRegex("^.*$"))
						)
						.connectHandler(serverFrame -> {
							logger.info("STOMP client connected " + serverFrame.frame());
						})
				)
				.listen(localPort, "localhost");
		logger.info("STOMP listening on " + stompServer.actualPort());
	}
}
