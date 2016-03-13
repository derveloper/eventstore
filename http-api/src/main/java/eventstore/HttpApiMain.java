package eventstore;

import eventstore.boundary.HttpApi;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

class HttpApiMain {
	public static void main(final String[] args) {
		VertxOptions options = new VertxOptions().setClustered(true);

		Vertx.clusteredVertx(options, res -> {
			if (res.succeeded()) {
				Vertx vertx = res.result();
				vertx.deployVerticle(new HttpApi());
			} else {
				// failed!
				System.out.println(res.cause().getMessage());
			}
		});
	}
}
