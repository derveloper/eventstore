package eventstore.boundary;

import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import eventstore.util.RethinkUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

import java.util.HashMap;

import static com.rethinkdb.RethinkDB.r;

public class PushApi extends AbstractVerticle {
	private static final String DBHOST = System.getenv("EVENTSTORE_RETHINKDB_ADDRESS") == null ? "localhost" : System.getenv("EVENTSTORE_RETHINKDB_ADDRESS");
	private Logger logger;
	private StompClientConnection stompClientConnection;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final EventBus eventBus = vertx.eventBus();

		logger.info("connecting to rethink on " + DBHOST);

		final Integer stompPort = config().getInteger("stomp.port");
		if (stompPort != null) {
			createStompClient(stompPort);
		}

		eventBus.consumer("event.subscribe", message -> {
			logger.debug("subscribing" + message.body());
			vertx.executeBlocking(fut -> {
				final JsonObject body = (JsonObject) message.body();
				final String address = (String) body.remove("address");

				logger.debug("creating changefeed for: " + address + " with body " + body.encode());

				Connection conn = null;
				try {
					conn = r.connection().hostname(DBHOST).connect();
					final MapObject mapObject = RethinkUtils.getMapObjectFromJson(body);
					final Cursor<HashMap<String, Object>> cur = r.db("eventstore").table("events")
							.filter(mapObject)
							.changes()
							.run(conn);
					logger.debug("created changefeed for " + mapObject);
					while (cur.hasNext()) {
						try {
							if (conn.isOpen()) {
								final HashMap<String, Object> next = cur.next();
								final Frame frame = new Frame();
								frame.setCommand(Frame.Command.SEND);
								frame.setDestination(address);
								final JsonObject newVal = new JsonObject(Json.encode(next.get("new_val")));
								final JsonObject data = newVal.getJsonObject("data");
								newVal.put("data", data.getJsonObject("map"));
								frame.setBody(Buffer.buffer(newVal.encodePrettily()));
								stompClientConnection.send(frame);
								logger.debug("publishing to: " + frame);
							}
						} catch (Exception e) {
							fut.fail(e);
						}
					}
					fut.complete();
				} catch (final Exception e) {
					fut.fail(e);
				} finally {
					if (conn != null && conn.isOpen()) conn.close();
				}
			}, ar -> {
				if (ar.failed()) {
					logger.error("changefeed failed", ar.cause());
				} else {
					logger.debug("got update!");
				}
			});
		});
	}

	private void createStompClient(Integer stompPort) {
		StompClient.create(vertx, new StompClientOptions()
				.setHeartbeat(new JsonObject().put("x", 1000).put("y", 0))
				.setHost("0.0.0.0").setPort(stompPort)
		).connect(ar -> {
			if (ar.succeeded()) {
				logger.debug("connected to STOMP");
				stompClientConnection = ar.result();
				stompClientConnection.pingHandler(stompClientConnection -> {
					logger.debug("ping from STOMP");
				});
				stompClientConnection.connectionDroppedHandler(stompClientConnection -> {
					logger.debug("connection dropped");
				});
				stompClientConnection.errorHandler(stompClientConnection -> {
					logger.debug("connection error");
				});
				stompClientConnection.closeHandler(stompClientConnection -> {
					logger.debug("connection close");
					createStompClient(stompPort);
				});
			} else {
				logger.warn("could not connect to STOMP", ar.cause());
			}
		});
	}
}
