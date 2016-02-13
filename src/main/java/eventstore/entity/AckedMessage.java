package eventstore.entity;

import io.vertx.core.json.JsonObject;

public class AckedMessage {
	public String replyAddress;
	public JsonObject message;

	public AckedMessage(String replyAddress, JsonObject message) {
		this.replyAddress = replyAddress;
		this.message = message;
	}

	public AckedMessage() {
	}
}
