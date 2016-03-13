package eventstore.boundary;

import eventstore.control.*;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
		deployBlocking(vertx, context, new DeploymentOptions().setConfig(new JsonObject().put("stomp.port", port2)).setWorker(true), PushApi.class.getName());
		deployBlocking(vertx, context, new JsonObject(), EventCacheVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject(), InMemoryEventPersistenceVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject(), WriteEventsVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject(), ReadEventsVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject().put("http.port", port), HttpApi.class.getName());
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
				.setHeartbeat(new JsonObject().put("x", 2000).put("y", 2000))
				.setHost("localhost").setPort(port2)
		)
				.connect(ar -> {
					if (ar.succeeded()) {
						System.out.println("connected to STOMP");
						final StompClientConnection connection = ar.result();
						final String address = testUrl(eventType) + "?eventType=" + eventType;
						System.out.println("subscribing to: " + address);
						connection.subscribe(address,
								frame -> {
									System.out.println("received frame: " + frame);
									context.assertEquals(
											new JsonObject(json).getJsonObject("data"),
											new JsonArray(frame.getBodyAsString()).getJsonObject(0).getJsonObject("data"));
									async.complete();
									connection.disconnect();
									connection.close();
								});
						vertx.executeBlocking(fut -> {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							vertx.createHttpClient().post(port, "localhost", testUrl(eventType))
									.putHeader("content-type", "application/json")
									.putHeader("content-length", length)
									.handler(response -> {
										context.assertEquals(response.statusCode(), 201);
										context.assertTrue(response.headers().get("content-type").contains("application/json"));
										fut.complete();
									})
									.write(json)
									.exceptionHandler(throwable -> context.asyncAssertFailure())
									.end();
						}, ar2 -> System.out.println("posted"));
					} else {
						//noinspection ThrowableResultOfMethodCallIgnored
						System.out.println("Failed to connect to the STOMP server: " + ar.cause().toString());
						context.asyncAssertFailure();
						async.complete();
					}
				});
	}

	private String testUrl(final String eventType) {
		return TEST_URL + eventType.split("-")[0];
	}
}
