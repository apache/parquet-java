/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.example.data.simple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

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
