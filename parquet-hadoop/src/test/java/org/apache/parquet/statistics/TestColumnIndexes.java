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

import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ColumnIndexValidator;
import org.apache.parquet.hadoop.ColumnIndexValidator.ContractViolation;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(Parameterized.class)
public class TestColumnIndexes {
  private static final int MAX_TOTAL_ROWS = 100_000;
  private static final MessageType SCHEMA = new MessageType("schema", new PrimitiveType(OPTIONAL, INT32, "i32"),
      new PrimitiveType(OPTIONAL, INT64, "i64"), new PrimitiveType(OPTIONAL, INT96, "i96"),
      new PrimitiveType(OPTIONAL, FLOAT, "sngl"), new PrimitiveType(OPTIONAL, DOUBLE, "dbl"),
      new PrimitiveType(OPTIONAL, BINARY, "strings"), new PrimitiveType(OPTIONAL, BINARY, "binary"),
      new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 17, "fixed-binary"),
      new PrimitiveType(REQUIRED, INT32, "unconstrained-i32"), new PrimitiveType(REQUIRED, INT64, "unconstrained-i64"),
      new PrimitiveType(REQUIRED, FLOAT, "unconstrained-sngl"),
      new PrimitiveType(REQUIRED, DOUBLE, "unconstrained-dbl"),
      Types.optional(INT32).as(intType(8, true)).named("int8"),
      Types.optional(INT32).as(intType(8, false)).named("uint8"),
      Types.optional(INT32).as(intType(16, true)).named("int16"),
      Types.optional(INT32).as(intType(16, false)).named("uint16"),
      Types.optional(INT32).as(intType(32, true)).named("int32"),
      Types.optional(INT32).as(intType(32, false)).named("uint32"),
      Types.optional(INT64).as(intType(64, true)).named("int64"),
      Types.optional(INT64).as(intType(64, false)).named("uint64"),
      Types.optional(INT32).as(decimalType(2, 9)).named("decimal-int32"),
      Types.optional(INT64).as(decimalType(4, 18)).named("decimal-int64"),
      Types.optional(FIXED_LEN_BYTE_ARRAY).length(19).as(decimalType(25, 45)).named("decimal-fixed"),
      Types.optional(BINARY).as(decimalType(20, 38)).named("decimal-binary"),
      Types.optional(BINARY).as(stringType()).named("utf8"), Types.optional(BINARY).as(enumType()).named("enum"),
      Types.optional(BINARY).as(jsonType()).named("json"), Types.optional(BINARY).as(bsonType()).named("bson"),
      Types.optional(INT32).as(dateType()).named("date"),
      Types.optional(INT32).as(timeType(true, TimeUnit.MILLIS)).named("time-millis"),
      Types.optional(INT64).as(timeType(false, TimeUnit.MICROS)).named("time-micros"),
      Types.optional(INT64).as(timestampType(true, TimeUnit.MILLIS)).named("timestamp-millis"),
      Types.optional(INT64).as(timestampType(false, TimeUnit.NANOS)).named("timestamp-nanos"),
      Types.optional(FIXED_LEN_BYTE_ARRAY).length(12).as(OriginalType.INTERVAL).named("interval"),
      Types.optional(BINARY).as(stringType()).named("always-null"));

  private static List<Supplier<?>> buildGenerators(int recordCount, Random random) {
    int fieldIndex = 0;
    return Arrays.<Supplier<?>>asList(
        sortedOrRandom(new RandomValues.IntGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.LongGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(RandomValues.int96Generator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.FloatGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.DoubleGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(RandomValues.binaryStringGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.BinaryGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.FixedGenerator(random.nextLong(), 17), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedIntGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedLongGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedFloatGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedDoubleGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.IntGenerator(random.nextLong(), Byte.MIN_VALUE, Byte.MAX_VALUE), random,
            recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.UIntGenerator(random.nextLong(), Byte.MIN_VALUE, Byte.MAX_VALUE), random,
            recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.IntGenerator(random.nextLong(), Short.MIN_VALUE, Short.MAX_VALUE), random,
            recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.UIntGenerator(random.nextLong(), Short.MIN_VALUE, Short.MAX_VALUE), random,
            recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedIntGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedIntGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedLongGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedLongGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedIntGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.UnconstrainedLongGenerator(random.nextLong()), random, recordCount,
            fieldIndex++),
        sortedOrRandom(new RandomValues.FixedGenerator(random.nextLong(), 19), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.BinaryGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(RandomValues.binaryStringGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(RandomValues.binaryStringGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(RandomValues.binaryStringGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.BinaryGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.IntGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.IntGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.LongGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.LongGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.LongGenerator(random.nextLong()), random, recordCount, fieldIndex++),
        sortedOrRandom(new RandomValues.FixedGenerator(random.nextLong(), 12), random, recordCount, fieldIndex++),
        null);
  }

  private static <T> Supplier<T> sortedOrRandom(Supplier<T> generator, Random random, int recordCount, int fieldIndex) {
    Comparator<T> cmp = SCHEMA.getType(fieldIndex).asPrimitiveType().comparator();

    // 20% chance for ascending, 20% for descending, 60% to remain random
    switch (random.nextInt(5)) {
    case 1:
      return RandomValues.wrapSorted(generator, recordCount, true, cmp);
    case 2:
      return RandomValues.wrapSorted(generator, recordCount, false, cmp);
    default:
      return generator;
    }
  }

  public static class WriteContext {
    private static final GroupFactory FACTORY = new SimpleGroupFactory(SCHEMA);
    private final long seed;
    private final int pageRowCountLimit;
    private final int columnIndexTruncateLength;

    private WriteContext(long seed, int pageRowCountLimit, int columnIndexTruncateLength) {
      this.seed = seed;
      this.pageRowCountLimit = pageRowCountLimit;
      this.columnIndexTruncateLength = columnIndexTruncateLength;
    }

    public Path write(Path directory) throws IOException {
      Path file = new Path(directory, "testColumnIndexes_" + this + ".parquet");
      Random random = new Random(seed);
      int recordCount = random.nextInt(MAX_TOTAL_ROWS) + 1;
      List<Supplier<?>> generators = buildGenerators(recordCount, random);
      Configuration conf = new Configuration();
      ParquetOutputFormat.setColumnIndexTruncateLength(conf, columnIndexTruncateLength);
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file).withType(SCHEMA)
          .withPageRowCountLimit(pageRowCountLimit).withConf(conf).build()) {
        for (int i = 0; i < recordCount; i++) {
          writer.write(createGroup(generators, random));
        }
      }
      return file;
    }

    private Group createGroup(List<Supplier<?>> generators, Random random) {
      Group group = FACTORY.newGroup();
      for (int column = 0, columnCnt = SCHEMA.getFieldCount(); column < columnCnt; ++column) {
        Type type = SCHEMA.getType(column);
        Supplier<?> generator = generators.get(column);
        // 2% chance of null value for an optional column
        if (generator == null || (type.isRepetition(OPTIONAL) && random.nextInt(50) == 0)) {
          continue;
        }
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
        case INT96:
          group.append(type.getName(), (Binary) generator.get());
          break;
        case INT32:
          group.append(type.getName(), (Integer) generator.get());
          break;
        case INT64:
          group.append(type.getName(), (Long) generator.get());
          break;
        case FLOAT:
          group.append(type.getName(), (Float) generator.get());
          break;
        case DOUBLE:
          group.append(type.getName(), (Double) generator.get());
          break;
        case BOOLEAN:
          group.append(type.getName(), (Boolean) generator.get());
          break;
        }
      }
      return group;
    }

    @Override
    public String toString() {
      return "seed=" + seed + ",pageRowCountLimit=" + pageRowCountLimit + ",columnIndexTruncateLength="
          + columnIndexTruncateLength;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TestColumnIndexes.class);

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Parameters
  public static Collection<WriteContext> getContexts() {
    return Arrays.asList(new WriteContext(System.nanoTime(), 1000, 8), new WriteContext(System.nanoTime(), 20000, 64),
        new WriteContext(System.nanoTime(), 50000, 10));
  }

  public TestColumnIndexes(WriteContext context) {
    this.context = context;
  }

  private final WriteContext context;

  @Test
  public void testColumnIndexes() throws IOException {
    LOGGER.info("Starting test with context: {}", context);

    Path file = null;
    try {
      file = context.write(new Path(tmp.getRoot().getAbsolutePath()));
      LOGGER.info("Parquet file \"{}\" is successfully created for the context: {}", file, context);

      List<ContractViolation> violations = ColumnIndexValidator
          .checkContractViolations(HadoopInputFile.fromPath(file, new Configuration()));
      assertTrue(violations.toString(), violations.isEmpty());
    } finally {
      if (file != null) {
        file.getFileSystem(new Configuration()).delete(file, false);
      }
    }
  }
}
