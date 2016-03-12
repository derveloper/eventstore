package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static eventstore.constants.Addresses.EVENT_SUBSCRIBE_ADDRESS;
import static eventstore.constants.Addresses.EVENT_UNSUBSCRIBE_ADDRESS;

public class PushApi extends AbstractVerticle {
	private Logger logger;
	private StompClientConnection stompClientConnection;
	private Map<String, Map.Entry<MessageConsumer<Object>, Integer>> subscriptions = new LinkedHashMap<>();
	private Map<String, String> clientToAddress = new LinkedHashMap<>();

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		final EventBus eventBus = vertx.eventBus();

		final Integer stompPort = config().getInteger("stomp.port");
		if (stompPort != null) {
			createStompClient(stompPort);
		}

		eventBus.consumer(EVENT_SUBSCRIBE_ADDRESS, message -> {
			logger.debug(String.format("subscribing %s", message.body()));
			final JsonObject body = (JsonObject) message.body();
			final String address = (String) body.remove("address");
			final String clientId = (String) body.remove("clientId");
			logger.debug(String.format("creating changefeed for: %s at %s with body %s", clientId, address, body.encode()));

			clientToAddress.put(clientId, address);
			if(subscriptions.containsKey(address)) {
				subscriptions.get(address).setValue(subscriptions.get(address).getValue() + 1);
				return;
			}

			MessageConsumer<Object> consumer = eventBus.consumer(address, objectMessage -> {
				final Frame frame = new Frame();
				frame.setCommand(Frame.Command.SEND);
				frame.setDestination(address);
				frame.setBody(Buffer.buffer(((JsonArray) objectMessage.body()).encodePrettily()));
				stompClientConnection.send(frame);
				logger.debug(String.format("publishing to: %s", frame));
			});
			subscriptions.put(address, new AbstractMap.SimpleEntry<>(consumer, 0));
		});

		eventBus.consumer(EVENT_UNSUBSCRIBE_ADDRESS, message -> {
			logger.debug(String.format("unsubscribing %s", message.body()));
			final String clientId = (String) message.body();
			final String address = clientToAddress.get(clientId);
			if(subscriptions.containsKey(address)) {
				subscriptions.get(address).setValue(subscriptions.get(address).getValue() - 1);
				if(subscriptions.get(address).getValue() == 0) {
					subscriptions.get(address).getKey().unregister();
					subscriptions.remove(address);
				}
			}
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
