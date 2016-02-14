package eventstore.boundary;

import eventstore.control.EventCacheVerticle;
import eventstore.control.EventPersistenceVerticle;
import eventstore.control.ReadEventsVerticle;
import eventstore.control.WriteEventsVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;

@RunWith(VertxUnitRunner.class)
public class ApiRouterTest {
	private Vertx vertx;
	private int port;

	@Before
	public void setUp(final TestContext context) throws IOException {
		vertx = Vertx.vertx();
		final ServerSocket socket = new ServerSocket(0);
		port = socket.getLocalPort();
		socket.close();

		vertx.deployVerticle(EventPersistenceVerticle.class.getName());
		vertx.deployVerticle(EventCacheVerticle.class.getName());
		vertx.deployVerticle(WriteEventsVerticle.class.getName());
		vertx.deployVerticle(ReadEventsVerticle.class.getName());

		vertx.deployVerticle(
				ApiRouter.class.getName(),
				new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)),
				context.asyncAssertSuccess());
	}

	@After
	public void tearDown(final TestContext context) {
		vertx.close(context.asyncAssertSuccess());
	}

	@Test
	public void shouldFetchCorrectEventAfterPostingIt(final TestContext context) {
		final Async async = context.async();
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String json = new JsonObject()
				.put("eventType", "createFoo")
				.put("data", data)
				.encodePrettily();
		final String length = Integer.toString(json.length());
		vertx.createHttpClient().post(port, "localhost", "/stream")
				.putHeader("content-type", "application/json")
				.putHeader("content-length", length)
				.handler(response -> {
					context.assertEquals(response.statusCode(), 201);
					context.assertTrue(response.headers().get("content-type").contains("application/json"));
					response.bodyHandler(buffer -> {
						final JsonObject jsonObject = new JsonObject(buffer.toString());
						final String id = jsonObject.getString("id");
						vertx.createHttpClient().getNow(port, "localhost", "/stream", response2 -> {
							context.assertEquals(response2.statusCode(), 200);
							context.assertEquals(response2.headers().get("content-type"), "application/json");
							response2.bodyHandler(body2 -> {
								final JsonArray jsonArray = new JsonArray(body2.toString());
								final Optional<Object> optional = jsonArray.stream()
										.filter(o -> {
											final JsonObject object = (JsonObject) o;
											return id.equals(object.getString("id"));
										})
										.findAny();
								context.assertTrue(optional.isPresent());
								optional.ifPresent(o -> context.assertTrue(((JsonObject) o).getJsonObject("data").equals(data)));
								async.complete();
							});
						});
					});
				})
				.write(json)
				.end();
	}

	@Test
	public void shouldQueryCorrectEventAfterPostingIt(final TestContext context) {
		final Async async = context.async();
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String json = new JsonObject()
				.put("eventType", "createFoo")
				.put("data", data)
				.encodePrettily();
		final String length = Integer.toString(json.length());
		vertx.createHttpClient().post(port, "localhost", "/stream")
				.putHeader("content-type", "application/json")
				.putHeader("content-length", length)
				.handler(response -> {
					context.assertEquals(response.statusCode(), 201);
					context.assertTrue(response.headers().get("content-type").contains("application/json"));
					response.bodyHandler(buffer -> {
						final JsonObject jsonObject = new JsonObject(buffer.toString());
						final String id = jsonObject.getString("id");
						vertx.createHttpClient().getNow(port, "localhost", "/stream?id=" + id, response2 -> {
							context.assertEquals(response2.statusCode(), 200);
							context.assertEquals(response2.headers().get("content-type"), "application/json");
							response2.bodyHandler(body2 -> {
								final JsonArray jsonArray = new JsonArray(body2.toString());
								context.assertTrue(jsonArray.getJsonObject(0).getJsonObject("data").equals(data));
								async.complete();
							});
						});
					});
				})
				.write(json)
				.end();
	}

	@Test
	public void shouldQueryCorrectEventAfterPostingItFromDB(final TestContext context) {
		final Async async = context.async();
		final JsonObject data = new JsonObject().put("foo", "bar");
		final String json = new JsonObject()
				.put("eventType", "createFoo")
				.put("data", data)
				.encodePrettily();
		final String length = Integer.toString(json.length());
		vertx.createHttpClient().post(port, "localhost", "/stream")
				.putHeader("content-type", "application/json")
				.putHeader("content-length", length)
				.handler(response -> {
					context.assertEquals(response.statusCode(), 201);
					context.assertTrue(response.headers().get("content-type").contains("application/json"));
					response.bodyHandler(buffer -> vertx.createHttpClient().getNow(port, "localhost", "/stream?eventType=createFoo", response2 -> {
						context.assertEquals(response2.statusCode(), 200);
						context.assertEquals(response2.headers().get("content-type"), "application/json");
						response2.bodyHandler(body2 -> {
							final JsonArray jsonArray = new JsonArray(body2.toString());
							context.assertTrue(jsonArray.getJsonObject(0).getJsonObject("data").equals(data));
							async.complete();
						});
					}));
				})
				.write(json)
				.end();
	}
}