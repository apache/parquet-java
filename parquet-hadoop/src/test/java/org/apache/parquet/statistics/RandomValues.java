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

package org.apache.parquet.statistics;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.parquet.io.api.Binary;

public class RandomValues {
  private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

  public abstract static class RandomValueGenerator<T extends Comparable<T>> implements Supplier<T> {
    private final Random random;

    protected RandomValueGenerator(long seed) {
      this.random = new Random(seed);
    }

    public boolean shouldGenerateNull() {
      return (random.nextInt(10) == 0);
    }

    public int randomInt() {
      return random.nextInt();
    }

    public int randomPositiveInt(int maximum) {
      // Maximum may be a random number (which may be negative).
      return random.nextInt(Math.abs(maximum) + 1);
    }

    public long randomLong() {
      return random.nextLong();
    }

    public long randomLong(long maximum) {
      return randomLong() % maximum;
    }

    public float randomFloat() {
      return random.nextFloat();
    }

    public float randomFloat(float maximum) {
      return random.nextFloat() % maximum;
    }

    public double randomDouble() {
      return random.nextDouble();
    }

    public double randomDouble(double maximum) {
      return random.nextDouble() % maximum;
    }

    public BigInteger randomInt96() {
      return new BigInteger(95, random);
    }

    public BigInteger randomInt96(BigInteger maximum) {
      BigInteger result;
      while ((result = randomInt96()).compareTo(maximum) > 0)
        ;
      return result;
    }

    public char randomLetter() {
      return ALPHABET.charAt(randomPositiveInt(ALPHABET.length() - 1));
    }

    public String randomString(int maxLength) {
      return randomFixedLengthString(randomPositiveInt(maxLength));
    }

    public String randomFixedLengthString(int length) {
      StringBuilder builder = new StringBuilder();
      for (int index = 0; index < length; index++) {
        builder.append(randomLetter());
      }

      return builder.toString();
    }

    public abstract T nextValue();

    @Override
    public T get() {
      return nextValue();
    }
  }

  public abstract static class RandomBinaryBase<T extends Comparable<T>> extends RandomValueGenerator<T> {
    protected final int bufferLength;
    protected final byte[] buffer;

    public RandomBinaryBase(long seed, int bufferLength) {
      super(seed);

      this.bufferLength = bufferLength;
      this.buffer = new byte[bufferLength];
    }

    public abstract Binary nextBinaryValue();

    public Binary asReusedBinary(byte[] data) {
      int length = Math.min(data.length, bufferLength);
      System.arraycopy(data, 0, buffer, 0, length);
      return Binary.fromReusedByteArray(data, 0, length);
    }
  }

  public static class IntGenerator extends RandomValueGenerator<Integer> {
    private final int minimum;
    private final int range;

    public IntGenerator(long seed) {
      super(seed);
      RandomRange<Integer> randomRange = new RandomRange<>(randomInt(), randomInt());
      this.minimum = randomRange.minimum();
      this.range = (randomRange.maximum() - this.minimum);
    }

    public IntGenerator(long seed, int minimum, int maximum) {
      super(seed);
      RandomRange<Integer> randomRange = new RandomRange<>(minimum, maximum);
      this.minimum = randomRange.minimum();
      this.range = randomRange.maximum() - this.minimum;
    }

    @Override
    public Integer nextValue() {
      return (minimum + randomPositiveInt(range));
    }
  }

  public static class UIntGenerator extends IntGenerator {
    private final int mask;

    public UIntGenerator(long seed, byte minimum, byte maximum) {
      super(seed, minimum, maximum);
      mask = 0xFF;
    }

    public UIntGenerator(long seed, short minimum, short maximum) {
      super(seed, minimum, maximum);
      mask = 0xFFFF;
    }

    @Override
    public Integer nextValue() {
      return super.nextValue() & mask;
    }
  }

  public static class UnconstrainedIntGenerator extends RandomValueGenerator<Integer> {
    public UnconstrainedIntGenerator(long seed) {
      super(seed);
    }

    @Override
    public Integer nextValue() {
      return randomInt();
    }
  }

  public static class LongGenerator extends RandomValueGenerator<Long> {
    private final RandomRange<Long> randomRange = new RandomRange<Long>(randomLong(), randomLong());
    private final long minimum = randomRange.minimum();
    private final long maximum = randomRange.maximum();
    private final long range = (maximum - minimum);

    public LongGenerator(long seed) {
      super(seed);
    }

    @Override
    public Long nextValue() {
      return (minimum + randomLong(range));
    }
  }

  public static class UnconstrainedLongGenerator extends RandomValueGenerator<Long> {
    public UnconstrainedLongGenerator(long seed) {
      super(seed);
    }

    @Override
    public Long nextValue() {
      return randomLong();
    }
  }

  public static class Int96Generator extends RandomBinaryBase<Binary> {
    private final RandomRange<Integer> randomRangeJulianDay = new RandomRange<>(randomInt(), randomInt());
    private final int minimumJulianDay = randomRangeJulianDay.minimum();
    private final int maximumJulianDay = randomRangeJulianDay.maximum();
    private final int rangeJulianDay = (maximumJulianDay - minimumJulianDay);

    private static final int INT_96_LENGTH = 12;

    public Int96Generator(long seed) {
      super(seed, INT_96_LENGTH);
    }

    @Override
    public Binary nextValue() {
      long timeOfDay = randomLong();
      int julianDay = minimumJulianDay + randomPositiveInt(rangeJulianDay);

      ByteBuffer.wrap(buffer)
          .order(ByteOrder.LITTLE_ENDIAN)
          .putLong(timeOfDay)
          .putInt(julianDay);

      return Binary.fromReusedByteArray(buffer, 0, INT_96_LENGTH);
    }

    @Override
    public Binary nextBinaryValue() {
      return nextValue();
    }
  }

  public static class FloatGenerator extends RandomValueGenerator<Float> {
    private final RandomRange<Float> randomRange = new RandomRange<Float>(randomFloat(), randomFloat());
    private final float minimum = randomRange.minimum();
    private final float maximum = randomRange.maximum();
    private final float range = (maximum - minimum);

    public FloatGenerator(long seed) {
      super(seed);
    }

    @Override
    public Float nextValue() {
      return (minimum + randomFloat(range));
    }
  }

  public static class UnconstrainedFloatGenerator extends RandomValueGenerator<Float> {
    public UnconstrainedFloatGenerator(long seed) {
      super(seed);
    }

    @Override
    public Float nextValue() {
      return randomFloat();
    }
  }

  public static class DoubleGenerator extends RandomValueGenerator<Double> {
    private final RandomRange<Double> randomRange = new RandomRange<Double>(randomDouble(), randomDouble());
    private final double minimum = randomRange.minimum();
    private final double maximum = randomRange.maximum();
    private final double range = (maximum - minimum);

    public DoubleGenerator(long seed) {
      super(seed);
    }

    @Override
    public Double nextValue() {
      return (minimum + randomDouble(range));
    }
  }

  public static class UnconstrainedDoubleGenerator extends RandomValueGenerator<Double> {
    public UnconstrainedDoubleGenerator(long seed) {
      super(seed);
    }

    @Override
    public Double nextValue() {
      return randomDouble();
    }
  }

  public static class StringGenerator extends RandomBinaryBase<String> {
    private static final int MAX_STRING_LENGTH = 16;

    public StringGenerator(long seed) {
      super(seed, MAX_STRING_LENGTH);
    }

    @Override
    public String nextValue() {
      int stringLength = randomPositiveInt(15) + 1;
      return randomString(stringLength);
    }

    @Override
    public Binary nextBinaryValue() {
      return asReusedBinary(nextValue().getBytes());
    }
  }

  public static class BinaryGenerator extends RandomBinaryBase<Binary> {
    private static final int MAX_STRING_LENGTH = 16;

    public BinaryGenerator(long seed) {
      super(seed, MAX_STRING_LENGTH);
    }

    @Override
    public Binary nextValue() {
      // use a random length, but ensure it is at least a few bytes
      int length = 5 + randomPositiveInt(buffer.length - 5);
      for (int index = 0; index < length; index++) {
        buffer[index] = (byte) randomInt();
      }

      return Binary.fromReusedByteArray(buffer, 0, length);
    }

    @Override
    public Binary nextBinaryValue() {
      return nextValue();
    }
  }

  public static class FixedGenerator extends RandomBinaryBase<Binary> {
    public FixedGenerator(long seed, int length) {
      super(seed, length);
    }

    @Override
    public Binary nextValue() {
      for (int index = 0; index < buffer.length; index++) {
        buffer[index] = (byte) randomInt();
      }

      return Binary.fromReusedByteArray(buffer);
    }

    @Override
    public Binary nextBinaryValue() {
      return nextValue();
    }
  }

  private static class RandomRange<T extends Comparable<T>> {
    private T minimum;
    private T maximum;

    public RandomRange(T lhs, T rhs) {
      this.minimum = lhs;
      this.maximum = rhs;

      if (minimum.compareTo(rhs) > 0) {
        T temporary = minimum;
        minimum = maximum;
        maximum = temporary;
      }
    }

    public T minimum() {
      return this.minimum;
    }

    public T maximum() {
      return this.maximum;
    }
  }

  public static Supplier<Binary> binaryStringGenerator(long seed) {
    final StringGenerator generator = new StringGenerator(seed);
    return generator::nextBinaryValue;
  }

  public static Supplier<Binary> int96Generator(long seed) {
    final Int96Generator generator = new Int96Generator(seed);
    return generator::nextBinaryValue;
  }

  public static <T extends Comparable<T>> Supplier<T> wrapSorted(
      Supplier<T> supplier, int recordCount, boolean ascending) {
    return wrapSorted(supplier, recordCount, ascending, (a, b) -> a.compareTo(b));
  }

  public static <T> Supplier<T> wrapSorted(
      Supplier<T> supplier, int recordCount, boolean ascending, Comparator<T> cmp) {
    List<T> values = new ArrayList<>(recordCount);
    for (int i = 0; i < recordCount; ++i) {
      values.add(supplier.get());
    }
    if (ascending) {
      values.sort(cmp);
    } else {
      values.sort((a, b) -> cmp.compare(b, a));
    }
    final Iterator<T> it = values.iterator();
    return it::next;
  }
}
