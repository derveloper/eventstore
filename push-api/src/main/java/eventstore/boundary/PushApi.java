package eventstore.boundary;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;


@ProxyGen
public interface PushApi {
  void subscribe(final String clientId, final String address);
  void unsubscribe(final String clientId);

  static PushApi createProxy(Vertx vertx, String address) {
    return new PushApiVertxEBProxy(vertx, address);
  }
}
