package eventstore;

import eventstore.boundary.ApiRouter;
import eventstore.boundary.PushApi;
import eventstore.boundary.StompBridge;
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
	public static void main(final String[] args) {
		final Vertx vertx = Vertx.vertx();
		try {
			final ServerSocket socket2 = new ServerSocket(0);
			final int stompPort = socket2.getLocalPort();
			socket2.close();

			vertx.deployVerticle(new StompBridge(), new DeploymentOptions().setConfig(new JsonObject().put("stomp.port", stompPort)), ar -> {
				if (ar.succeeded()) {
					vertx.deployVerticle(new PushApi(), new DeploymentOptions().setConfig(new JsonObject().put("stomp.port", stompPort)).setWorker(true));
					vertx.deployVerticle(new EventCacheVerticle());
					vertx.deployVerticle(new EventPersistenceVerticle(), new DeploymentOptions().setConfig(new JsonObject()
							.put("stomp.port", stompPort)
					));
					vertx.deployVerticle(new WriteEventsVerticle());
					vertx.deployVerticle(new ReadEventsVerticle());
					vertx.deployVerticle(new ApiRouter());
				}
			});
		} catch (final IOException e) {
			System.out.println("could not allocate free port!");
			System.exit(-1);
		}
	}
}
