package eventstore.boundary;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;


class Helper {
  public static void deployBlocking(final Vertx vertx, final TestContext context, final DeploymentOptions stompConfig,
                                    final String deployUnit) throws InterruptedException {
    deployBlocking(vertx, context, stompConfig.getConfig(), deployUnit);
  }

  public static void deployBlocking(final Vertx vertx, final TestContext context, final JsonObject stompConfig, final String deployUnit)
      throws InterruptedException {
    final Async async = context.async();
    vertx.deployVerticle(deployUnit, new DeploymentOptions().setConfig(stompConfig), stringAsyncResult -> {
      if (stringAsyncResult.succeeded()) {
        async.complete();
      }
    });
    async.awaitSuccess();
    Thread.sleep(100);
    System.out.println("deployed " + deployUnit);
  }
}
