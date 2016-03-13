package eventstore.persistence;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import io.vertx.core.json.JsonObject;

public class RethinkUtils {
	public static MapObject getMapObjectFromJson(final JsonObject body) {
		final MapObject mapObject = RethinkDB.r.hashMap();
		body.forEach(o -> mapObject.with(o.getKey(), o.getValue()));
		return mapObject;
	}
}
