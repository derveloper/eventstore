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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.UUID;

import static eventstore.boundary.Helper.deployBlocking;

@RunWith(VertxUnitRunner.class)
public class ApiRouterTest {
	private static final String TEST_URL = "/stream/test";
	private Vertx vertx;
	private int port;
	private static MongodProcess MONGO;
	private static int MONGO_PORT = 27018;

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
	public void shouldFetchCorrectEventAfterPostingIt(final TestContext context) {
		final Async async = context.async();
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String eventType = UUID.randomUUID().toString();
		final String json = new JsonObject()
				.put("eventType", eventType)
				.put("data", data)
				.encodePrettily();
		final String length = Integer.toString(json.length());
		vertx.createHttpClient().post(port, "localhost", testUrl(eventType))
				.putHeader("content-type", "application/json")
				.putHeader("content-length", length)
				.handler(response -> {
					context.assertEquals(response.statusCode(), 201);
					context.assertTrue(response.headers().get("content-type").contains("application/json"));
					response.bodyHandler(buffer -> {
						final JsonArray jsonArray = new JsonArray(buffer.toString());
						final String id = jsonArray.getJsonObject(0).getString("id");
						vertx.createHttpClient().get(port, "localhost", testUrl(eventType), response2 -> {
							context.assertEquals(response2.statusCode(), 200);
							context.assertEquals(response2.headers().get("content-type"), "application/json");
							response2.bodyHandler(body2 -> {
								final JsonArray jsonArray2 = new JsonArray(body2.toString());
								final Optional<Object> optional = jsonArray2.stream()
										.filter(o -> {
											final JsonObject object = (JsonObject) o;
											return id.equals(object.getString("id"));
										})
										.findAny();
								context.assertTrue(optional.isPresent());
								optional.ifPresent(o -> context.assertTrue(((JsonObject) o).getJsonObject("data").equals(data)));
								async.complete();
							}).exceptionHandler(throwable -> context.asyncAssertFailure());
						}).exceptionHandler(throwable -> context.asyncAssertFailure()).end();
					});
				})
				.write(json)
				.exceptionHandler(throwable -> context.asyncAssertFailure())
				.end();
	}

	@Test
	public void shouldQueryCorrectEventAfterPostingIt(final TestContext context) {
		final Async async = context.async();
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String eventType = UUID.randomUUID().toString();
		final String json = new JsonObject()
				.put("eventType", eventType)
				.put("data", data)
				.encodePrettily();
		final String length = Integer.toString(json.length());
		vertx.createHttpClient().post(port, "localhost", testUrl(eventType))
				.putHeader("content-type", "application/json")
				.putHeader("content-length", length)
				.handler(response -> {
					context.assertEquals(response.statusCode(), 201);
					context.assertTrue(response.headers().get("content-type").contains("application/json"));
					response.bodyHandler(buffer -> {
						final JsonArray jsonArray1 = new JsonArray(buffer.toString());
						final String id = jsonArray1.getJsonObject(0).getString("id");
						vertx.createHttpClient().get(port, "localhost", testUrl(eventType) + "?id=" + id, response2 -> {
							context.assertEquals(response2.statusCode(), 200);
							context.assertEquals(response2.headers().get("content-type"), "application/json");
							response2.bodyHandler(body2 -> {
								final JsonArray jsonArray = new JsonArray(body2.toString());
								context.assertTrue(jsonArray.getJsonObject(0).getJsonObject("data").equals(data));
								async.complete();
							})
							.exceptionHandler(throwable -> context.asyncAssertFailure());
						}).exceptionHandler(throwable -> context.asyncAssertFailure()).end();
					});
				})
				.write(json)
				.exceptionHandler(throwable -> context.asyncAssertFailure())
				.end();
	}

	private String testUrl(final String eventType) {
		return TEST_URL + eventType.split("-")[0];
	}

	@Test
	public void shouldQueryCorrectEventAfterPostingItFromDB(final TestContext context) {
		final Async async = context.async();
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String eventType = UUID.randomUUID().toString();
		final String json = new JsonObject()
				.put("eventType", eventType)
				.put("data", data)
				.encodePrettily();
		final String length = Integer.toString(json.length());
		vertx.createHttpClient().post(port, "localhost", testUrl(eventType))
				.putHeader("content-type", "application/json")
				.putHeader("content-length", length)
				.handler(response -> {
					context.assertEquals(response.statusCode(), 201);
					context.assertTrue(response.headers().get("content-type").contains("application/json"));
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					response.bodyHandler(buffer -> vertx.createHttpClient().get(port, "localhost", testUrl(eventType) + "?eventType=" + eventType, response2 -> {
						context.assertEquals(response2.statusCode(), 200);
						context.assertEquals(response2.headers().get("content-type"), "application/json");
						response2.bodyHandler(body2 -> {
							final JsonArray jsonArray = new JsonArray(body2.toString());
							context.assertTrue(jsonArray.getJsonObject(0).getJsonObject("data").equals(data));
							async.complete();
						});
					}).exceptionHandler(throwable -> context.asyncAssertFailure()).end());
				})
				.write(json)
				.exceptionHandler(throwable -> context.asyncAssertFailure())
				.end();
	}
}