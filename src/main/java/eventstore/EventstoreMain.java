package eventstore;

import eventstore.boundary.ApiRouter;
import eventstore.control.EventCacheVerticle;
import eventstore.control.ReadEventsVerticle;
import eventstore.control.WriteEventsVerticle;
import io.vertx.core.Vertx;

public class EventstoreMain {
	public static void main(String[] args) {
		final Vertx vertx = Vertx.vertx();

		vertx.deployVerticle(new EventCacheVerticle());
		vertx.deployVerticle(new WriteEventsVerticle());
		vertx.deployVerticle(new ReadEventsVerticle());
		vertx.deployVerticle(new ApiRouter());
	}
}
