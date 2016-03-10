package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

public class PushApi extends AbstractVerticle {
	private Logger logger;
	private StompClientConnection stompClientConnection;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final EventBus eventBus = vertx.eventBus();

		final Integer stompPort = config().getInteger("stomp.port");
		if (stompPort != null) {
			createStompClient(stompPort);
		}

		eventBus.consumer("event.subscribe", message -> {
			logger.debug("subscribing" + message.body());
			final JsonObject body = (JsonObject) message.body();
			final String address = (String) body.remove("address");
			logger.debug("creating changefeed for: " + address + " with body " + body.encode());

			eventBus.consumer(address, objectMessage -> {
				final Frame frame = new Frame();
				frame.setCommand(Frame.Command.SEND);
				frame.setDestination(address);
				frame.setBody(Buffer.buffer(((JsonArray) objectMessage).encodePrettily()));
				stompClientConnection.send(frame);
				logger.debug("publishing to: " + frame);
			});
		});
	}

	private void createStompClient(Integer stompPort) {
		StompClient.create(vertx, new StompClientOptions()
				.setHeartbeat(new JsonObject().put("x", 1000).put("y", 0))
				.setHost("0.0.0.0").setPort(stompPort)
		).connect(ar -> {
			if (ar.succeeded()) {
				logger.debug("connected to STOMP");
				stompClientConnection = ar.result();
				stompClientConnection.pingHandler(stompClientConnection -> {
					logger.debug("ping from STOMP");
				});
				stompClientConnection.connectionDroppedHandler(stompClientConnection -> {
					logger.debug("connection dropped");
				});
				stompClientConnection.errorHandler(stompClientConnection -> {
					logger.debug("connection error");
				});
				stompClientConnection.closeHandler(stompClientConnection -> {
					logger.debug("connection close");
					createStompClient(stompPort);
				});
			} else {
				logger.warn("could not connect to STOMP", ar.cause());
			}
		});
	}
}
