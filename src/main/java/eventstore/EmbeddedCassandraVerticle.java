package eventstore;

import io.vertx.core.AbstractVerticle;
import org.apache.cassandra.service.EmbeddedCassandraService;

import java.io.IOException;

class EmbeddedCassandraVerticle extends AbstractVerticle {
    private final EmbeddedCassandraService cassandra;

    EmbeddedCassandraVerticle() {
        cassandra = new EmbeddedCassandraService();
    }

    public void start() throws IOException {
        vertx.executeBlocking(objectFuture -> {
            try {
                cassandra.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, tAsyncResult -> { });
    }
}
