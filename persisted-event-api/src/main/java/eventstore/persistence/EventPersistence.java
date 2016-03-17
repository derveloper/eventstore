package eventstore.persistence;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


@ProxyGen
public interface EventPersistence {
  void read(final JsonObject query, final Handler<AsyncResult<JsonArray>> result);
  static EventPersistence createProxy(final Vertx vertx, final String address) {
    return new EventPersistenceVertxEBProxy(vertx, address);
  }

  void write(final JsonArray events, final Handler<AsyncResult<Boolean>> result);
}
