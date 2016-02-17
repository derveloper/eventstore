package eventstore.boundary;

import eventstore.control.EventCacheVerticle;
import eventstore.control.EventPersistenceVerticle;
import eventstore.control.ReadEventsVerticle;
import eventstore.control.WriteEventsVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;

import static eventstore.boundary.Helper.deployBlocking;


@RunWith(VertxUnitRunner.class)
public class StompBridgeTest {
	private static final String TEST_URL = "/stream/test";
	private Vertx vertx;
	private int port;
	private int port2;

	@Before
	public void setUp(final TestContext context) throws IOException, InterruptedException {
		vertx = Vertx.vertx();
		final ServerSocket socket = new ServerSocket(0);
		port = socket.getLocalPort();
		socket.close();
		final ServerSocket socket2 = new ServerSocket(0);
		port2 = socket2.getLocalPort();
		socket2.close();

		deployBlocking(vertx, context, new JsonObject().put("stomp.port", port2), StompBridge.class.getName());
		deployBlocking(vertx, context, new JsonObject(), EventPersistenceVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject(), EventCacheVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject(), WriteEventsVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject(), ReadEventsVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject().put("http.port", port), ApiRouter.class.getName());
	}

	@After
	public void tearDown(final TestContext context) {
		vertx.close(context.asyncAssertSuccess());
	}

	@Test
	public void shouldPublishEventOverSTOMP(final TestContext context) throws InterruptedException {
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String eventType = UUID.randomUUID().toString();
		final String json = new JsonObject()
				.put("eventType", eventType)
				.put("data", data)
				.encodePrettily();
		final Async async = context.async();
		final String length = Integer.toString(json.length());
		StompClient.create(vertx, new StompClientOptions()
				.setHeartbeat(new JsonObject().put("x", 10000).put("y", 10000))
				.setHost("localhost").setPort(port2)
		)
				.connect(ar -> {
					if (ar.succeeded()) {
						System.out.println("connected to STOMP");
						StompClientConnection connection = ar.result();
						connection.subscribe("write.store.events.persisted",
								frame -> {
									System.out.println("Just received a frame from /queue : " + frame);
									connection.disconnect();
									connection.close();
									context.asyncAssertSuccess();
									async.complete();
								});
						vertx.createHttpClient().post(port, "localhost", testUrl(eventType))
								.putHeader("content-type", "application/json")
								.putHeader("content-length", length)
								.handler(response -> {
									context.assertEquals(response.statusCode(), 201);
									context.assertTrue(response.headers().get("content-type").contains("application/json"));
									async.complete();
								})
								.write(json)
								.end();
					} else {
						System.out.println("Failed to connect to the STOMP server: " + ar.cause().toString());
						context.asyncAssertFailure();
						async.complete();
					}
				});
	}

	private String testUrl(String eventType) {
		return TEST_URL + eventType.split("-")[0];
	}
}