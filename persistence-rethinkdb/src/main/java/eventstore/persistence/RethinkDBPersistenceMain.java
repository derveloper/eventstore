package eventstore.persistence;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

class RethinkDBPersistenceMain {
	public static void main(final String[] args) {
		VertxOptions options = new VertxOptions().setClustered(true);

		Vertx.clusteredVertx(options, res -> {
			if (res.succeeded()) {
				Vertx vertx = res.result();
				vertx.deployVerticle(new RethinkDBEventPersistenceVerticle());
			} else {
				// failed!
				System.out.println(res.cause().getMessage());
			}
		});
	}
}
