package eventstore.shared.service;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;


@ProxyGen
@VertxGen
public interface PushApi {
  void subscribe(final String clientId, final String address);

  static PushApi createProxy(Vertx vertx, String address) {
    return new eventstore.shared.service.PushApiVertxEBProxy(vertx, address);
  }
}
