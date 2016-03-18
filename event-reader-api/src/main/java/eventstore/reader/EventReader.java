package eventstore.reader;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


@ProxyGen
public interface EventReader {
  void read(final JsonObject query, final Handler<AsyncResult<JsonArray>> result);

  static EventReader createProxy(Vertx vertx, String address) {
    return new EventReaderVertxEBProxy(vertx, address);
  }
}
