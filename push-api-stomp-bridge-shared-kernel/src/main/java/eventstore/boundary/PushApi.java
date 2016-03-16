package eventstore.boundary;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;


@ProxyGen
@VertxGen
public interface PushApi {
  void subscribe(final String clientId, final String address);
  void unsubscribe(final String clientId);

  static PushApi createProxy(Vertx vertx, String address) {
    return new eventstore.boundary.PushApiVertxEBProxy(vertx, address);
  }
}
