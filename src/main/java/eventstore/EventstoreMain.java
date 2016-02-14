package eventstore;

import eventstore.boundary.ApiRouter;
import eventstore.control.EventCacheVerticle;
import eventstore.control.EventPersistenceVerticle;
import eventstore.control.ReadEventsVerticle;
import eventstore.control.WriteEventsVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.net.ServerSocket;

public class EventstoreMain {
	public static void main(String[] args) {
		final Vertx vertx = Vertx.vertx();
		try {
			ServerSocket socket = new ServerSocket(0);
			final int localPort = socket.getLocalPort();
			socket.close();

			vertx.deployVerticle(new EventCacheVerticle());
			vertx.deployVerticle(new EventPersistenceVerticle());
			vertx.deployVerticle(new WriteEventsVerticle());
			vertx.deployVerticle(new ReadEventsVerticle());
			vertx.deployVerticle(new ApiRouter(), new DeploymentOptions().setConfig(new JsonObject().put("http.port", localPort)));
		} catch (IOException e) {
			System.out.println("could not allocate free port!");
			System.exit(-1);
		}
	}
}
