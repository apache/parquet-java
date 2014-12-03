package parquet.example.data.simple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import parquet.Preconditions;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;

public class NanoTime extends Primitive {
  private final int julianDay;
  private final long timeOfDayNanos;

  public static NanoTime fromBinary(Binary bytes) {
    Preconditions.checkArgument(bytes.length() == 12, "Must be 12 bytes");
    ByteBuffer buf = bytes.toByteBuffer();
    buf.order(ByteOrder.LITTLE_ENDIAN);
    long timeOfDayNanos = buf.getLong();
    int julianDay = buf.getInt();
    return new NanoTime(julianDay, timeOfDayNanos);
  }

  public static NanoTime fromInt96(Int96Value int96) {
    ByteBuffer buf = int96.getInt96().toByteBuffer();
    return new NanoTime(buf.getInt(), buf.getLong());
  }

  public NanoTime(int julianDay, long timeOfDayNanos) {
    this.julianDay = julianDay;
    this.timeOfDayNanos = timeOfDayNanos;
  }

  public int getJulianDay() {
    return julianDay;
  }

  public long getTimeOfDayNanos() {
    return timeOfDayNanos;
  }

  public Binary toBinary() {
    ByteBuffer buf = ByteBuffer.allocate(12);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(timeOfDayNanos);
    buf.putInt(julianDay);
    buf.flip();
    return Binary.fromByteBuffer(buf);
  }

  public Int96Value toInt96() {
    return new Int96Value(toBinary());
  }

  @Override
  public void writeValue(RecordConsumer recordConsumer) {
    recordConsumer.addBinary(toBinary());
  }

  @Override
  public String toString() {
    return "NanoTime{julianDay="+julianDay+", timeOfDayNanos="+timeOfDayNanos+"}";
  }
}
