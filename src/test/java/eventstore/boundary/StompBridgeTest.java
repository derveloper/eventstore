package eventstore.boundary;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import eventstore.control.EventCacheVerticle;
import eventstore.control.EventPersistenceVerticle;
import eventstore.control.ReadEventsVerticle;
import eventstore.control.WriteEventsVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
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
	private static MongodProcess MONGO;
	private static int MONGO_PORT = 27020;

	@BeforeClass
	public static void initialize() throws IOException {
		System.setProperty("EVENTSTORE_MONGODB_HOSTS", "mongodb://127.0.0.1:" + MONGO_PORT);
		MongodStarter starter = MongodStarter.getDefaultInstance();
		IMongodConfig mongodConfig = new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net(MONGO_PORT, Network.localhostIsIPv6()))
				.build();
		MongodExecutable mongodExecutable =
				starter.prepare(mongodConfig);
		MONGO = mongodExecutable.start();
	}

	@AfterClass
	public static void shutdown() {  MONGO.stop(); }

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
		deployBlocking(vertx, context, new JsonObject(), EventCacheVerticle.class.getName());
		deployBlocking(vertx, context, new JsonObject().put("stomp.port", port2), EventPersistenceVerticle.class.getName());
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
									context.assertEquals(
											new JsonObject(json).getJsonObject("data"),
											new JsonObject(frame.getBodyAsString()).getJsonObject("data"));
									async.complete();
									connection.disconnect();
									connection.close();
								});
						vertx.createHttpClient().post(port, "localhost", testUrl(eventType))
								.putHeader("content-type", "application/json")
								.putHeader("content-length", length)
								.handler(response -> {
									context.assertEquals(response.statusCode(), 201);
									context.assertTrue(response.headers().get("content-type").contains("application/json"));
								})
								.write(json)
								.exceptionHandler(throwable -> context.asyncAssertFailure())
								.end();
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