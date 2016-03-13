package eventstore.writer;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

class WriteEventsMain {
	public static void main(final String[] args) {

		ClusterManager mgr = new HazelcastClusterManager();

		VertxOptions options = new VertxOptions().setClusterManager(mgr);

		Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();
                vertx.deployVerticle(new WriteEventsVerticle());
            } else {
                // failed!
                System.out.println(res.cause().getMessage());
            }
        });
	}
}
