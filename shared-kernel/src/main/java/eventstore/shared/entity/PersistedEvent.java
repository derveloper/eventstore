package eventstore.shared.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;


@SuppressWarnings("WeakerAccess")
public class PersistedEvent implements Serializable {
  public final String streamName;
  public final String id;
  public final String eventType;
  public final Date createdAt;
  public final Object data;

  public PersistedEvent(final String id, final String streamName, final String eventType, final Object data) {
    this.id = id == null
              ? UUID.randomUUID().toString()
              : id;
    this.streamName = streamName;
    this.createdAt = new Date();
    this.eventType = eventType;
    this.data = data;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, streamName, eventType, createdAt, data);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    final PersistedEvent that = (PersistedEvent) o;
    return Objects.equals(id, that.id) &&
           Objects.equals(eventType, that.eventType) &&
           Objects.equals(createdAt, that.createdAt) &&
           Objects.equals(data, that.data);
  }

  @Override
  public String toString() {
    return "PersistedEvent{" +
           "id=" + id +
           ", streamName='" + streamName + '\'' +
           ", eventType='" + eventType + '\'' +
           ", createdAt=" + createdAt +
           ", data=" + data +
           '}';
  }
}
