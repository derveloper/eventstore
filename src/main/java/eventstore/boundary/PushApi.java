package eventstore.boundary;

import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
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
	private Logger logger;
	public static final String DBHOST = "172.17.0.2";
	private StompClientConnection stompClientConnection;

	@Override
	public void start() throws Exception {
		logger = LoggerFactory.getLogger(getClass() + "_" + deploymentID());
		final EventBus eventBus = vertx.eventBus();

		final Integer stompPort = config().getInteger("stomp.port");
		if (stompPort != null) {
			StompClient.create(vertx, new StompClientOptions()
					.setHost("0.0.0.0").setPort(stompPort)
			).connect(ar -> {
				if (ar.succeeded()) {
					logger.debug("connected to STOMP");
					stompClientConnection = ar.result();
				} else {
					logger.warn("could not connect to STOMP", ar.cause());
				}
			});
		}

		eventBus.consumer("event.subscribe", message -> {
			final JsonObject body = (JsonObject) message.body();

			final String address = (String) body.remove("address");
			vertx.executeBlocking(fut -> {
				Connection conn = null;
				try {
					conn = r.connection().hostname(DBHOST).connect();
					Cursor<HashMap<String, Object>> cur = r.db("eventstore").table("events")
							.changes()
							.filter(row -> {
								body.forEach(e -> row.g(e.getKey()).eq(e.getValue()));
								return row;
							})
							.run(conn);
					while (cur.hasNext()) {
						final Frame frame = new Frame();
						frame.setCommand(Frame.Command.SEND);
						frame.setDestination(address);
						final JsonObject newVal = new JsonObject(Json.encode(cur.next().get("new_val")));
						final JsonObject data = newVal.getJsonObject("data");
						newVal.put("data", data.getJsonObject("map"));
						frame.setBody(Buffer.buffer(newVal.encodePrettily()));
						stompClientConnection.send(frame);
						logger.debug("publishing to: " + frame);
					}
				} catch (Exception e) {
					//fut.fail(e);
				} finally {
					if(conn != null && conn.isOpen()) conn.close();
				}
			}, ar -> {
				if(ar.failed()) {
					logger.error("Error: changefeed failed", ar.cause());
				}
				else {
					logger.info("got update!");
				}
			});
		});
	}
}
