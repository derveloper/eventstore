package eventstore.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

public class PersistedEvent implements Serializable {
	public String id;
	public String eventType;
	public Date createdAt;
	public Object data;

	public PersistedEvent() {
	}

	public PersistedEvent(String eventType, Object data) {
		this.id = UUID.randomUUID().toString();
		this.createdAt = new Date();
		this.eventType = eventType;
		this.data = data;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PersistedEvent that = (PersistedEvent) o;
		return Objects.equals(id, that.id) &&
				Objects.equals(eventType, that.eventType) &&
				Objects.equals(createdAt, that.createdAt) &&
				Objects.equals(data, that.data);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, eventType, createdAt, data);
	}

	@Override
	public String toString() {
		return "PersistedEvent{" +
				"id=" + id +
				", eventType='" + eventType + '\'' +
				", createdAt=" + createdAt +
				", data=" + data +
				'}';
	}
}
