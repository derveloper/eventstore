package eventstore.boundary;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClientConnection;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;


public class PushApiImpl implements PushApi {
  private final Map<String, Map.Entry<MessageConsumer<Object>, Integer>> subscriptions = new LinkedHashMap<>();
  private final Map<String, String> clientToAddress = new LinkedHashMap<>();
  private Logger logger;
  private final StompClientConnection stompClientConnection;
  private EventBus eventBus;

  public PushApiImpl(StompClientConnection stompClientConnection, EventBus eventBus) {
    logger = LoggerFactory.getLogger(String.format("%s_%s", getClass(), "push-api"));
    this.stompClientConnection = stompClientConnection;
    this.eventBus = eventBus;
  }

  public void subscribe(final String clientId, final String address) {
    logger.debug(String.format("creating changefeed for: %s at %s", clientId, address));

    clientToAddress.put(clientId, address);
    if (subscriptions.containsKey(address)) {
      subscriptions.get(address).setValue(subscriptions.get(address).getValue() + 1);
      return;
    }

    final MessageConsumer<Object> consumer = eventBus.consumer(address, objectMessage -> {
      final Frame frame = new Frame();
      frame.setCommand(Frame.Command.SEND);
      frame.setDestination(address);
      if (objectMessage.body() instanceof JsonObject) {
        frame.setBody(Buffer.buffer(((JsonObject) objectMessage.body()).encodePrettily()));
      }
      else if (objectMessage.body() instanceof JsonArray) {
        frame.setBody(Buffer.buffer(((JsonArray) objectMessage.body()).encodePrettily()));
      }
      else {
        frame.setBody(Buffer.buffer());
      }
      stompClientConnection.send(frame);
      logger.debug(String.format("publishing to: %s", frame));
    });
    subscriptions.put(address, new AbstractMap.SimpleEntry<>(consumer, 0));
  }

  public void unsubscribe(final String clientId) {
    logger.debug(String.format("unsubscribing %s", clientId));
    if (clientToAddress.containsKey(clientId)) {
      final String address = clientToAddress.get(clientId);
      if (subscriptions.containsKey(address)) {
        subscriptions.get(address).setValue(subscriptions.get(address).getValue() - 1);
        if (subscriptions.get(address).getValue() == 0) {
          subscriptions.get(address).getKey().unregister();
          subscriptions.remove(address);
        }
      }
    }
  }
}
