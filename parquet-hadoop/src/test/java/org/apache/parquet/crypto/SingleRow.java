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
package org.apache.parquet.crypto;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.statistics.RandomValues;

public class SingleRow {
  private static final int RANDOM_SEED = 42;
  private static final int FIXED_LENGTH = 10;
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final RandomValues.IntGenerator intGenerator = new RandomValues.IntGenerator(RANDOM_SEED);
  private static final RandomValues.FloatGenerator floatGenerator = new RandomValues.FloatGenerator(RANDOM_SEED);
  private static final RandomValues.DoubleGenerator doubleGenerator = new RandomValues.DoubleGenerator(RANDOM_SEED);
  private static final RandomValues.BinaryGenerator binaryGenerator = new RandomValues.BinaryGenerator(RANDOM_SEED);
  private static final RandomValues.FixedGenerator fixedBinaryGenerator =
      new RandomValues.FixedGenerator(RANDOM_SEED, FIXED_LENGTH);

  public static final String BOOLEAN_FIELD_NAME = "boolean_field";
  public static final String INT32_FIELD_NAME = "int32_field";
  public static final String FLOAT_FIELD_NAME = "float_field";
  public static final String DOUBLE_FIELD_NAME = "double_field";
  public static final String BINARY_FIELD_NAME = "ba_field";
  public static final String FIXED_LENGTH_BINARY_FIELD_NAME = "flba_field";
  public static final String PLAINTEXT_INT32_FIELD_NAME = "plain_int32_field";

  private static final MessageType SCHEMA = new MessageType(
      "schema",
      new PrimitiveType(REQUIRED, BOOLEAN, BOOLEAN_FIELD_NAME),
      new PrimitiveType(REQUIRED, INT32, INT32_FIELD_NAME),
      new PrimitiveType(REQUIRED, FLOAT, FLOAT_FIELD_NAME),
      new PrimitiveType(REQUIRED, DOUBLE, DOUBLE_FIELD_NAME),
      new PrimitiveType(OPTIONAL, BINARY, BINARY_FIELD_NAME),
      Types.required(FIXED_LEN_BYTE_ARRAY).length(FIXED_LENGTH).named(FIXED_LENGTH_BINARY_FIELD_NAME),
      new PrimitiveType(OPTIONAL, INT32, PLAINTEXT_INT32_FIELD_NAME));

  public final boolean boolean_field;
  public final int int32_field;
  public final float float_field;
  public final double double_field;
  public final byte[] ba_field;
  public final byte[] flba_field;
  public final Integer plaintext_int32_field; // Can be null, since it doesn't exist in C++-created files yet.

  public SingleRow(
      boolean boolean_field,
      int int32_field,
      float float_field,
      double double_field,
      byte[] ba_field,
      byte[] flba_field,
      Integer plaintext_int32_field) {
    this.boolean_field = boolean_field;
    this.int32_field = int32_field;
    this.float_field = float_field;
    this.double_field = double_field;
    this.ba_field = ba_field;
    this.flba_field = flba_field;
    this.plaintext_int32_field = plaintext_int32_field;
  }

  public static MessageType getSchema() {
    return SCHEMA;
  }

  public static List<SingleRow> generateRandomData(int rowCount) {
    List<SingleRow> dataList = new ArrayList<>(rowCount);
    for (int row = 0; row < rowCount; ++row) {
      SingleRow newRow = new SingleRow(
          RANDOM.nextBoolean(),
          intGenerator.nextValue(),
          floatGenerator.nextValue(),
          doubleGenerator.nextValue(),
          binaryGenerator.nextValue().getBytes(),
          fixedBinaryGenerator.nextValue().getBytes(),
          intGenerator.nextValue());
      dataList.add(newRow);
    }
    return dataList;
  }

  public static List<SingleRow> generateLinearData(int rowCount) {
    List<SingleRow> dataList = new ArrayList<>(rowCount);
    String baseStr = "parquet";
    for (int row = 0; row < rowCount; ++row) {
      boolean boolean_val = ((row % 2) == 0) ? true : false;
      float float_val = (float) row * 1.1f;
      double double_val = (row * 1.1111111);

      byte[] binary_val = null;
      if ((row % 2) == 0) {
        char firstChar = (char) ((int) '0' + row / 100);
        char secondChar = (char) ((int) '0' + (row / 10) % 10);
        char thirdChar = (char) ((int) '0' + row % 10);
        binary_val = (baseStr + firstChar + secondChar + thirdChar).getBytes(StandardCharsets.UTF_8);
      }
      char[] fixed = new char[FIXED_LENGTH];
      char[] aChar = Character.toChars(row);
      Arrays.fill(fixed, aChar[0]);

      SingleRow newRow = new SingleRow(
          boolean_val,
          row,
          float_val,
          double_val,
          binary_val,
          new String(fixed).getBytes(StandardCharsets.UTF_8),
          null /*plaintext_int32_field*/);
      dataList.add(newRow);
    }
    return dataList;
  }
}
