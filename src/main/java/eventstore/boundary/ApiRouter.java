package eventstore.boundary;

import eventstore.entity.AckedMessage;
import io.netty.util.CharsetUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

public class ApiRouter extends AbstractVerticle {
	private EventBus eventBus;
	private final Logger logger;

	public ApiRouter() {
		logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void start() throws Exception {
		eventBus = vertx.eventBus();
		eventBus.registerDefaultCodec(AckedMessage.class, new MessageCodec<AckedMessage, Object>() {
			public void encodeToWire(final Buffer buffer, final AckedMessage ackedMessage) {
				final String strJson = Json.encode(ackedMessage);
				final byte[] encoded = strJson.getBytes(CharsetUtil.UTF_8);
				buffer.appendInt(encoded.length);
				final Buffer buff = Buffer.buffer(encoded);
				buffer.appendBuffer(buff);
			}

			public String decodeFromWire(int pos, final Buffer buffer) {
				final int length = buffer.getInt(pos);
				pos += 4;
				final byte[] encoded = buffer.getBytes(pos, pos + length);
				return new String(encoded, CharsetUtil.UTF_8);
			}

			public String transform(final AckedMessage ackedMessage) {
				return Json.encode(ackedMessage);
			}

			public String name() {
				return "AckedMessageCodec";
			}

			public byte systemCodecID() {
				return (byte) -1;
			}
		});
		final HttpServer httpServer = vertx.createHttpServer();
		final Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());

		router.post("/stream/*").handler(sendMessage("write.events", false));
		router.get("/stream/*").handler(sendMessage("read.events", true));

		listen(httpServer, router);
	}

	private Handler<RoutingContext> sendMessage(String address, boolean respondWithReply) {
		return routingContext -> {
			if(respondWithReply) {
				final String message;
				if(routingContext.getBody().length() > 0) {
					message = routingContext.getBodyAsJson().encodePrettily();
				}
				else {
					message = "{}";
				}

				eventBus.send(address, message, reply -> {
					if (reply.succeeded()) {
						routingContext.response().end((String) reply.result().body());
					}
				});
			}
			else {
				routingContext.response().end();
				eventBus.send(address, routingContext.getBodyAsString());
			}
		};
	}

	private void listen(HttpServer httpServer, Router router) throws IOException {
		ServerSocket socket = new ServerSocket(0);
		final int localPort = socket.getLocalPort();
		socket.close();

		logger.info("Listening on " + localPort);

		httpServer.requestHandler(router::accept).listen(localPort);
	}
}
