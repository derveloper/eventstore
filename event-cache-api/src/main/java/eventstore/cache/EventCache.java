package eventstore.cache;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


@ProxyGen
public interface EventCache {
  void read(final JsonObject query, final Handler<AsyncResult<JsonArray>> result);
  void write(final JsonArray events);
  static EventCache createProxy(Vertx vertx, String address) {
    return new EventCacheVertxEBProxy(vertx, address);
  }
}
