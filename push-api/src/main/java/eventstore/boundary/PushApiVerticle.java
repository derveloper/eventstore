package eventstore.boundary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;
import io.vertx.serviceproxy.ProxyHelper;

import java.util.LinkedHashMap;
import java.util.Map;

import static eventstore.shared.constants.Addresses.EVENT_UNSUBSCRIBE_ADDRESS;
import static eventstore.shared.constants.SharedDataKeys.EVENTSTORE_CONFIG_MAP;
import static eventstore.shared.constants.SharedDataKeys.STOMP_BRIDGE_ADDRESS_KEY;


public class PushApiVerticle extends AbstractVerticle {
  private final Map<String, Map.Entry<MessageConsumer<Object>, Integer>> subscriptions = new LinkedHashMap<>();
  private final Map<String, String> clientToAddress = new LinkedHashMap<>();
  private Logger logger;
  private StompClientConnection stompClientConnection;
  private EventBus eventBus;

  @Override
  public void start() throws Exception {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), deploymentID()));
    eventBus = vertx.eventBus();

    locateStompBridgeAndConnect(eventBus);
  }

  private void locateStompBridgeAndConnect(final EventBus eventBus) {
    if (config().getString("stomp.address", null) != null) {
      connectToStomp(eventBus, config().getString("stomp.address"));
    }
    else {
      final SharedData sd = vertx.sharedData();
      sd.<String, String>getClusterWideMap(EVENTSTORE_CONFIG_MAP, res -> {
        if (res.succeeded()) {
          final AsyncMap<String, String> map = res.result();
          map.get(STOMP_BRIDGE_ADDRESS_KEY, resGet -> {
            if (resGet.succeeded()) {
              // Successfully got the value
              final String stompAddress = resGet.result();
              connectToStomp(eventBus, stompAddress);
            }
            else {
              logger.error("failed getting stomp-address", resGet.cause());
              locateStompBridgeAndConnect(eventBus);
            }
          });
        }
        else {
          logger.error("failed getting stomp-address", res.cause());
          locateStompBridgeAndConnect(eventBus);
        }
      });
    }
  }

  private void connectToStomp(EventBus eventBus, String stompAddress) {
    if (stompAddress == null) {
      locateStompBridgeAndConnect(eventBus);
      return;
    }
    logger.info(String.format("got stomp-address %s", stompAddress));
    final Integer stompPort = config().getInteger("stomp.port", 8091);
    if (stompPort != null) {
      createStompClient(stompAddress, stompPort);
    }

    eventBus.consumer(EVENT_UNSUBSCRIBE_ADDRESS, message -> {

    });
  }

  private void createStompClient(final String address, final Integer stompPort) {
    StompClient
        .create(vertx,
                new StompClientOptions()
                    .setHeartbeat(new JsonObject().put("x", 1000).put("y", 0))
                    .setHost(address).setPort(stompPort))
        .connect(ar -> {
          if (ar.succeeded()) {
            logger.info("connected to STOMP");
            stompClientConnection = ar.result();
            stompClientConnection.closeHandler(stompClientConnection -> {
              logger.info("connection close");
              createStompClient(address, stompPort);
            });
            PushApiImpl service = new PushApiImpl(stompClientConnection, eventBus);
            ProxyHelper.registerService(PushApi.class, vertx, service, "push-api");
          }
          else {
            logger.error("could not connect to STOMP", ar.cause());
          }
        });
  }
}
