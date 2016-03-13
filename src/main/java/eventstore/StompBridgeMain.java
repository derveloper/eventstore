package eventstore;

import eventstore.boundary.StompBridge;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.io.IOException;
import java.net.ServerSocket;

class StompBridgeMain {
	public static void main(final String[] args) {
		ClusterManager mgr = new HazelcastClusterManager();

		VertxOptions options = new VertxOptions().setClusterManager(mgr);

		Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();
                final ServerSocket socket2;
                try {
                    socket2 = new ServerSocket(0);
                    final int stompPort = socket2.getLocalPort();
                    socket2.close();
                    vertx.deployVerticle(new StompBridge(), new DeploymentOptions().setConfig(new JsonObject().put("stomp.port", stompPort)));
                } catch (IOException e) {
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
