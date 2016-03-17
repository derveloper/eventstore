package eventstore.writer;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;


@ProxyGen
public interface EventWriter {
  void write(final JsonArray events);

  static EventWriter createProxy(Vertx vertx, String address) {
    return new EventWriterVertxEBProxy(vertx, address);
  }
}
