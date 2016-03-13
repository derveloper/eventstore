package eventstore.boundary;

import eventstore.shared.constants.MessageFields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static eventstore.shared.constants.Addresses.EVENT_SUBSCRIBE_ADDRESS;
import static eventstore.shared.constants.Addresses.EVENT_UNSUBSCRIBE_ADDRESS;
import static eventstore.shared.constants.MessageFields.EVENT_ADDRESS_FIELD;
import static eventstore.shared.constants.MessageFields.EVENT_CLIENT_ID_FIELD;
import static eventstore.shared.constants.SharedDataKeys.EVENTSTORE_CONFIG_MAP;
import static eventstore.shared.constants.SharedDataKeys.STOMP_BRIDGE_ADDRESS_KEY;

public class PushApi extends AbstractVerticle {
	private Logger logger;
	private StompClientConnection stompClientConnection;
	private Map<String, Map.Entry<MessageConsumer<Object>, Integer>> subscriptions = new LinkedHashMap<>();
	private Map<String, String> clientToAddress = new LinkedHashMap<>();

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
		final EventBus eventBus = vertx.eventBus();

		locateStompBridgeAndConnect(eventBus);
	}

	private void locateStompBridgeAndConnect(final EventBus eventBus) {
		final SharedData sd = vertx.sharedData();
		sd.<String, String>getClusterWideMap(EVENTSTORE_CONFIG_MAP, res -> {
			if (res.succeeded()) {
				final AsyncMap<String, String> map = res.result();
				map.get(STOMP_BRIDGE_ADDRESS_KEY, resGet -> {
					if (resGet.succeeded()) {
						// Successfully got the value
						final String stompAddress = resGet.result();
						if(stompAddress == null) {
							locateStompBridgeAndConnect(eventBus);
							return;
						}
						logger.info(String.format("got stomp-address %s", stompAddress));
						final Integer stompPort = config().getInteger("stomp.port", 8091);
						if (stompPort != null) {
							createStompClient(stompAddress, stompPort);
						}

						eventBus.consumer(EVENT_SUBSCRIBE_ADDRESS, message -> {
							logger.debug(String.format("subscribing %s", message.body()));
							final JsonObject body = (JsonObject) message.body();
							final String address = (String) body.remove(EVENT_ADDRESS_FIELD);
							final String clientId = (String) body.remove(EVENT_CLIENT_ID_FIELD);
							logger.debug(String.format("creating changefeed for: %s at %s with body %s", clientId, address, body.encode()));

							clientToAddress.put(clientId, address);
							if(subscriptions.containsKey(address)) {
								subscriptions.get(address).setValue(subscriptions.get(address).getValue() + 1);
								return;
							}

							final MessageConsumer<Object> consumer = eventBus.consumer(address, objectMessage -> {
								final Frame frame = new Frame();
								frame.setCommand(Frame.Command.SEND);
								frame.setDestination(address);
								frame.setBody(Buffer.buffer(((JsonObject) objectMessage.body()).encodePrettily()));
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
					} else {
						logger.error("failed getting stomp-address", resGet.cause());
						locateStompBridgeAndConnect(eventBus);
					}
				});
			} else {
				logger.error("failed getting stomp-address", res.cause());
				locateStompBridgeAndConnect(eventBus);
			}
		});
	}

	private void createStompClient(final String address, final Integer stompPort) {
		StompClient.create(vertx, new StompClientOptions()
				.setHeartbeat(new JsonObject().put("x", 1000).put("y", 0))
				.setHost(address).setPort(stompPort)
		).connect(ar -> {
			if (ar.succeeded()) {
				logger.info("connected to STOMP");
				stompClientConnection = ar.result();
				stompClientConnection.closeHandler(stompClientConnection -> {
					logger.info("connection close");
					createStompClient(address, stompPort);
				});
			} else {
				logger.error("could not connect to STOMP", ar.cause());
			}
		});
	}
}
