package eventstore.boundary;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class Helper {
	public static void deployBlocking(Vertx vertx, TestContext context, JsonObject stompConfig, String deployUnit) throws InterruptedException {
		Async async = context.async();
		vertx.deployVerticle(deployUnit, new DeploymentOptions().setConfig(stompConfig), stringAsyncResult -> {
			if(stringAsyncResult.succeeded()) {
				async.complete();
			}
		});
		async.awaitSuccess();
		Thread.sleep(100);
		System.out.println("deployed " + deployUnit);
	}
}
