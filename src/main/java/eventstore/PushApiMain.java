package eventstore;

import eventstore.boundary.PushApi;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.io.IOException;
import java.net.ServerSocket;

class PushApiMain {
	public static void main(final String[] args) {
		ClusterManager mgr = new HazelcastClusterManager();

		VertxOptions options = new VertxOptions().setClusterManager(mgr);

		Vertx.clusteredVertx(options, res -> {
			if (res.succeeded()) {
				Vertx vertx = res.result();
				try {
					final ServerSocket socket2 = new ServerSocket(0);
					final int stompPort = socket2.getLocalPort();
					socket2.close();

					vertx.deployVerticle(new PushApi(), new DeploymentOptions().setConfig(new JsonObject().put("stomp.port", stompPort)).setWorker(true));
				} catch (final IOException e) {
					System.out.println("could not allocate free port!");
					System.exit(-1);
				}
			} else {
				// failed!
				System.out.println(res.cause().getMessage());
			}
		});
	}
}
