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

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigInteger;
import java.util.*;

public class RandomValueGenerators {
  private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

  public static abstract class RandomValueGenerator<T extends Comparable<T>> {
    protected final Random random;
    protected final PrimitiveType.PrimitiveTypeName forType;

    protected RandomValueGenerator(long seed, PrimitiveType.PrimitiveTypeName forType) {
      this.random = new Random(seed);
      this.forType = forType;
    }

    public boolean shouldGenerateNull() {
      return (random.nextInt(10) == 0);
    }

    public int randomInt() { return randomInt(Integer.MAX_VALUE - 1); }
    public int randomInt(int maximum) {
      // Maximum may be a random number (which may be negative).
      return random.nextInt(Math.abs(maximum) + 1);
    }

    public long randomLong() { return random.nextLong(); }
    public long randomLong(long maximum) { return randomLong() % maximum; }

    public float randomFloat() { return random.nextFloat(); }
    public float randomFloat(float maximum) { return random.nextFloat() % maximum; }

    public double randomDouble() { return random.nextDouble(); }
    public double randomDouble(double maximum) { return random.nextDouble() % maximum; }

    public BigInteger randomInt96(Random random) {
      return new BigInteger(95, random);
    }

    public BigInteger randomInt96(BigInteger maximum, Random random) {
      BigInteger result;
      while ((result = randomInt96(random)).compareTo(maximum) > 0);
      return result;
    }

    public char randomLetter() {
      return ALPHABET.charAt(randomInt() % ALPHABET.length());
    }

    public String randomString(int maxLength) {
      return randomFixedLengthString(randomInt(maxLength));
    }

    public String randomFixedLengthString(int length) {
      StringBuilder builder = new StringBuilder();
      for (int index = 0; index < length; index++) {
        builder.append(randomLetter());
      }

      return builder.toString();
    }

    public int randomInt(int low, int high) {
      int range = (high - low);
      return low + randomInt(range);
    }

    protected abstract T nextValue();
  }

  public static abstract class RandomBinaryIterable<T extends Comparable<T>> extends RandomValueGenerator<T> {
    protected final int bufferLength;
    protected final byte[] buffer;

    public RandomBinaryIterable(long seed, PrimitiveType.PrimitiveTypeName forType, int bufferLength) {
      super(seed, forType);

      this.bufferLength = bufferLength;
      this.buffer = new byte[bufferLength];
    }

    public abstract Binary nextBinaryValue();

    public Binary asReusedBinary(byte[] data) {
      Arrays.fill(buffer, (byte)0);
      System.arraycopy(data, 0, buffer, 0, Math.min(data.length, bufferLength));
      return Binary.fromReusedByteArray(data);
    }
  }

  public static class RandomIntGenerator extends RandomValueGenerator<Integer> {
    private final RandomRange<Integer> randomRange = new RandomRange<Integer>(randomInt(), randomInt());
    private final int minimum = randomRange.minimum();
    private final int maximum = randomRange.maximum();
    private final int range = (maximum - minimum);

    public RandomIntGenerator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.INT32);
    }

    @Override
    protected Integer nextValue() {
      if (!shouldGenerateNull()) {
        return (minimum + randomInt(range));
      } else {
        return null;
      }
    }
  }

  public static class RandomLongGenerator extends RandomValueGenerator<Long> {
    private final RandomRange<Long> randomRange = new RandomRange<Long>(randomLong(), randomLong());
    private final long minimum = randomRange.minimum();
    private final long maximum = randomRange.maximum();
    private final long range = (maximum - minimum);

    public RandomLongGenerator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.INT64);
    }

    @Override
    protected Long nextValue() {
      if (!shouldGenerateNull()) {
        return (minimum + randomLong(range));
      } else {
        return null;
      }
    }
  }

  public static class RandomInt96Generator extends RandomBinaryIterable<BigInteger> {
    private final RandomRange<BigInteger> randomRange = new RandomRange<BigInteger>(randomInt96(random), randomInt96(random));
    private final BigInteger minimum = randomRange.minimum();
    private final BigInteger maximum = randomRange.maximum();
    private final BigInteger range = maximum.subtract(minimum);

    private static final int INT_96_LENGTH = 12;

    public RandomInt96Generator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.INT96, INT_96_LENGTH);
    }

    @Override
    protected BigInteger nextValue() {
      if (!shouldGenerateNull()) {
        return (minimum.add(randomInt96(range, super.random)));
      } else {
        return null;
      }
    }

    @Override
    public Binary nextBinaryValue() {
      BigInteger value = nextValue();
      if (value != null) {
        return asReusedBinary(value.toByteArray());
      } else {
        return null;
      }
    }
  }


  public static class RandomFloatGenerator extends RandomValueGenerator<Float> {
    private final RandomRange<Float> randomRange = new RandomRange<Float>(randomFloat(), randomFloat());
    private final float minimum = randomRange.minimum();
    private final float maximum = randomRange.maximum();
    private final float range = (maximum - minimum);

    public RandomFloatGenerator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.FLOAT);
    }

    @Override
    protected Float nextValue() {
      if (!shouldGenerateNull()) {
        return (minimum + randomFloat(range));
      } else {
        return null;
      }
    }
  }

  public static class RandomDoubleGenerator extends RandomValueGenerator<Double> {
    private final RandomRange<Double> randomRange = new RandomRange<Double>(randomDouble(), randomDouble());
    private final double minimum = randomRange.minimum();
    private final double maximum = randomRange.maximum();
    private final double range = (maximum - minimum);

    public RandomDoubleGenerator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.DOUBLE);
    }

    @Override
    protected Double nextValue() {
      if (!shouldGenerateNull()) {
        return (minimum + randomDouble(range));
      } else {
        return null;
      }
    }
  }

  public static class RandomStringGenerator extends RandomBinaryIterable<String> {
    private static final int MAX_STRING_LENGTH = 16;
    public RandomStringGenerator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.BINARY, MAX_STRING_LENGTH);
    }

    @Override
    protected String nextValue() {
      int stringLength = randomInt(15) + 1;

      if (!shouldGenerateNull()) {
        return randomString(stringLength);
      } else {
        return null;
      }
    }

    @Override
    public Binary nextBinaryValue() {
      String value = nextValue();
      if (value != null) {
        return asReusedBinary(value.getBytes());
      } else {
        return null;
      }
    }
  }

  public static class RandomBinaryGenerator extends RandomBinaryIterable<Binary> {
    private static final int MAX_STRING_LENGTH = 16;
    public RandomBinaryGenerator(long seed) {
      super(seed, PrimitiveType.PrimitiveTypeName.BINARY, MAX_STRING_LENGTH);
    }

    @Override
    protected Binary nextValue() {
      if (!shouldGenerateNull()) {
        for (int index = 0; index < bufferLength; index++) {
          buffer[index] = (byte)randomInt();
        }

        return Binary.fromReusedByteArray(buffer);
      } else {
        return null;
      }
    }

    @Override
    public Binary nextBinaryValue() {
      Binary value = nextValue();
      if (value != null) {
        return value;
      } else {
        return null;
      }
    }
  }

  public static class RandomFixedBinaryGenerator extends RandomBinaryIterable<String> {
    public RandomFixedBinaryGenerator(long seed, int length) {
      super(seed, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, length);
    }

    @Override
    protected String nextValue() {
      if (!shouldGenerateNull()) {
        return randomFixedLengthString(bufferLength);
      } else {
        return null;
      }
    }

    @Override
    public Binary nextBinaryValue() {
      String value = nextValue();
      if (value != null) {
        return asReusedBinary(value.getBytes());
      } else {
        return null;
      }
    }
  }

  public static class RandomRange<T extends Comparable<T>> {
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

    public T minimum() { return this.minimum; }
    public T maximum() { return this.maximum; }
  }
}
