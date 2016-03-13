package eventstore;

import eventstore.boundary.HttpApi;
import eventstore.boundary.PushApi;
import eventstore.boundary.StompBridge;
import eventstore.control.EventCacheVerticle;
import eventstore.control.ReadEventsVerticle;
import eventstore.control.RethinkDBEventPersistenceVerticle;
import eventstore.control.WriteEventsVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.io.IOException;
import java.net.ServerSocket;

class HttpApiMain {
	public static void main(final String[] args) {
		ClusterManager mgr = new HazelcastClusterManager();

		VertxOptions options = new VertxOptions().setClusterManager(mgr);

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
