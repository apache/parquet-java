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
package org.apache.parquet.benchmarks;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.apache.parquet.variant.VariantValueWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for {@link VariantBuilder}: construction, serialization and deserialization of
 * Variant objects.
 *
 * <p>The benchmark mirrors the structure of the Iceberg {@code VariantSerializationBenchmark} so
 * that results from the two projects can be compared directly. Parameters are kept identical where
 * the APIs permit:
 *
 * <ul>
 *   <li>{@link #fieldCount} – total number of top-level fields per object.
 *   <li>{@link #depth} – {@code Shallow} (primitives only) or {@code Nested} (some fields are
 *       5-field sub-objects).
 * </ul>
 *
 * <p>Unlike the Iceberg benchmark there is no "shredded percent" axis: parquet-java's
 * {@link VariantBuilder} constructs unshredded Variant binary directly; shredding is handled
 * separately by the Parquet writer layer.
 *
 * <p>Build and run:
 *
 * <pre>
 *   ./mvnw --projects parquet-benchmarks -amd -DskipTests -Denforcer.skip=true clean package
 *   ./parquet-benchmarks/run.sh all org.apache.parquet.benchmarks.VariantBenchmark \
 *       -wi 5 -i 5 -f 1 -rff /tmp/variant-benchmark.json
 * </pre>
 *
 * Change fork to 1 before merge
 */
@Fork(0)
@State(Scope.Benchmark)
@Warmup(iterations = 100)
@Measurement(iterations = 100)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantBenchmark {

  /** Whether to include nested sub-objects in the field values. */
  public enum Depth {
    /** Flat structure: no nesting. */
    Flat,
    /** Nested values. */
    Nested,
  }

  /**
   * Iterations on the small benchmarks whose operations are so fast that clocks, especially ARM clocks,
   * can't reliably measure them.
   */
  private static final int ITERATIONS = 100;

  /** Total number of top-level fields in each variant object. */
  @Param({"1000", "10000"})
  private int fieldCount;

  /** Whether to include nested variant objects as some of the field values. */
  @Param({"Flat", "Nested"})
  private Depth depth;

  /**
   * A counter of strings created; used to ensure limited uniqueness in strings.
   */
  private static int counter;

  /**
   * Get a count value.
   * @return a new value.
   */
  private static int count() {
    int c = counter++;
    if (c >= 512) {
      c = 0;
    }
    return c;
  }
  /**
   * Type of a field and the operations to (a) append an instance of that type to
   * the variant builder and (b) add the type to a GroupBuilder.
   */
  private enum FieldType {
    String((o, builder) -> builder.appendString(((String) o) + count()), b -> b.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("typed_value")),
    Int((o, builder) -> builder.appendInt((Integer) o), b -> b.optional(PrimitiveTypeName.INT32)
        .named("typed_value")),
    Long((o, builder) -> builder.appendLong((Long) o), b -> b.optional(PrimitiveTypeName.INT64)
        .named("typed_value")),
    Float((o, builder) -> builder.appendFloat((Float) o), b -> b.optional(PrimitiveTypeName.FLOAT)
        .named("typed_value")),
    Double((o, builder) -> builder.appendDouble((Double) o), b -> b.optional(PrimitiveTypeName.DOUBLE)
        .named("typed_value")),
    BigDecimal((o, builder) -> builder.appendDecimal((BigDecimal) o), b -> b.optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.decimalType(0, 9))
        .named("typed_value")),
    UUID((o, builder) -> builder.appendUUID((UUID) o), b -> b.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(16)
        .as(LogicalTypeAnnotation.uuidType())
        .named("typed_value")),
    /** Nested MUST be the last in the enum. */
    Nested(
        (o, builder) -> {
          throw new UnsupportedOperationException("Nested object");
        },
        b -> {
          /* falls back to value column */
        });

    /**
     * Append an object during variant construction.
     */
    final BiConsumer<Object, VariantObjectBuilder> append;

    final Consumer<Types.GroupBuilder<GroupType>> addTypedValue;

    FieldType(
        final BiConsumer<Object, VariantObjectBuilder> append,
        final Consumer<Types.GroupBuilder<GroupType>> addTypedValue) {
      this.append = append;
      this.addTypedValue = addTypedValue;
    }

    void append(Object o, VariantObjectBuilder builder) {
      append.accept(o, builder);
    }

    void addTypedValue(Types.GroupBuilder<GroupType> builder) {
      addTypedValue.accept(builder);
    }
  }

  /**
   * Each field entry is its type and the value.
   */
  private static final class FieldEntry {
    final FieldType type;
    final Object value;

    public FieldEntry(final FieldType type, final Object value) {
      this.type = type;
      this.value = value;
    }
  }

  /** Number of fields in each nested sub-object in a nested variant. */
  private static final int NESTED_FIELD_COUNT = 5;

  // ---- state built once per trial ----

  /** Ordered list of top-level field names, e.g. "field_0" … "field_N-1". */
  private List<String> fieldNames;

  /**
   * Some types are pregenerated to keep RNG costs out of the benchmark, placed in generic object map then
   * cast to the correct type.
   */
  private FieldEntry[] fieldValues;

  /**
   * Indices of fields that are strings, used when constructing nested sub-objects so that nested
   * fields share the top-level field-name dictionary.
   */
  private int[] stringFieldIndices;

  private int stringFieldCount;

  /**
   * A pre-built {@link Variant} for all benchmarks which want to keep build costs out
   * of their measurements.
   */
  private Variant preBuiltVariant;

  /**
   * Fixed random seed for reproducibility across runs. The same seed is used in the Iceberg
   * benchmark.
   */
  private Random random;

  /** Shredded schema for the variant, built from field types in setup. */
  private GroupType shreddedSchema;

  /** Unshredded schema (metadata + value only, no typed_value). */
  private GroupType unshreddedSchema;

  /** No-op RecordConsumer that discards all output. */
  private RecordConsumer noopConsumer;

  // ------------------------------------------------------------------
  // Setup
  // ------------------------------------------------------------------

  /**
   * Build all benchmark state. Called once per parameter combination before any iterations run.
   *
   * <p>Field values are pre-decided (type tags + numeric arrays) so benchmark methods are free of
   * allocation and RNG cost outside what VariantBuilder itself does.
   */
  @Setup(Level.Trial)
  public void setupTrial() {
    random = new Random(0x1ceb1cebL);

    // --- field names ---
    fieldNames = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      fieldNames.add("field_" + i);
    }

    // --- pre-generate typed values ---
    // Type distribution: biased towards strings.

    int typeCount = FieldType.Nested.ordinal();
    if (depth == Depth.Flat) {
      typeCount--;
    }

    List<Integer> stringIndices = new ArrayList<>();

    fieldValues = new FieldEntry[fieldCount];
    for (int i = 0; i < fieldCount; i++) {

      // slightly more than the type count as there are extra strings
      int typeIndex = random.nextInt(typeCount + 2);
      // based on type, create entries.
      FieldEntry fieldEntry;
      switch (typeIndex) {
        case 0:
          fieldEntry = new FieldEntry(FieldType.String, "string-");
          break;
        case 1:
          fieldEntry = new FieldEntry(FieldType.String, "longer string-");
          break;
        case 2:
          fieldEntry =
              new FieldEntry(FieldType.String, "a longer string assuming these will be more common #");
          break;
        case 3: // no char option here
          fieldEntry = new FieldEntry(FieldType.String, "a");
          break;
        case 4:
          fieldEntry = new FieldEntry(FieldType.Int, random.nextInt());
          break;
        case 5:
          fieldEntry = new FieldEntry(FieldType.Long, random.nextLong());
          break;
        case 6:
          fieldEntry = new FieldEntry(FieldType.Float, random.nextFloat());
          break;
        case 7:
          fieldEntry = new FieldEntry(FieldType.Double, random.nextDouble());
          break;
        case 8:
          fieldEntry = new FieldEntry(FieldType.UUID, UUID.randomUUID());
          break;
        case 9:
          fieldEntry = new FieldEntry(FieldType.Nested, null);
          break;
        default:
          throw new AssertionError("out of range: " + typeIndex);
      }
      // safety check
      Objects.requireNonNull(fieldEntry, "field entry is null");
      fieldValues[i] = fieldEntry;
      if (fieldEntry.type == FieldType.String) {
        stringIndices.add(i);
      }
    }

    stringFieldCount = stringIndices.size();
    stringFieldIndices = new int[stringFieldCount];
    for (int i = 0; i < stringFieldCount; i++) {
      stringFieldIndices[i] = stringIndices.get(i);
    }

    // --- pre-built variant for deserialization benchmark ---
    preBuiltVariant = buildVariant();
    // for writing
    noopConsumer = new NoopRecordConsumer();

    // --- schemas for shredding benchmarks ---
    unshreddedSchema = Types.buildGroup(Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .named("variant_field");
    shreddedSchema = buildShreddedSchema();
  }

  // ------------------------------------------------------------------
  // Benchmark methods
  // ------------------------------------------------------------------

  /**
   * Create a {@link VariantBuilder} from scratch, append all fields, call {@link
   * VariantBuilder#build()}. Measures object construction including dictionary encoding.
   */
  @Benchmark
  public void benchmarkBuildVariant(Blackhole bh) {
    for (int i = 0; i < ITERATIONS; i++) {
      Variant v = buildVariant();
      bh.consume(v.getValueBuffer());
      bh.consume(v.getMetadataBuffer());
    }
  }

  /**
   * Serialize-only: re-serializes the pre-built variant value buffer. Isolates the cost of
   * extracting the encoded bytes from a finished Variant without paying for construction.
   *
   * <p>Because {@link Variant#getValueBuffer()} returns the existing buffer, this benchmark
   * primarily measures the ByteBuffer access and the Blackhole overhead..
   */
  @Benchmark
  public void benchmarkSerialize(Blackhole bh) {
    // duplicate() gives an independent position/limit on the same backing array –
    for (int i = 0; i < ITERATIONS; i++) {
      // equivalent to the Iceberg benchmark's outputBuffer.clear() + writeTo() pattern.
      ByteBuffer value = preBuiltVariant.getValueBuffer().duplicate();
      ByteBuffer meta = preBuiltVariant.getMetadataBuffer().duplicate();
      bh.consume(value);
      bh.consume(meta);
    }
  }

  /**
   * Read path: iterate all fields of the pre-built variant, extracting each value. This exercises
   * the field-name lookup and type dispatch that a query engine performs on every row.
   */
  @Benchmark
  public void benchmarkDeserialize(Blackhole bh) {
    for (int j = 0; j < ITERATIONS; j++) {
      Variant v = preBuiltVariant;
      int n = v.numObjectElements();
      for (int i = 0; i < n; i++) {
        Variant.ObjectField field = v.getFieldAtIndex(i);
        bh.consume(field.key);
        bh.consume(field.value.getValueBuffer());
      }
    }
  }

  /**
   * Shred the pre-built variant into a fully typed schema. Measures the cost of type dispatch,
   * field matching, and recursive decomposition that {@link VariantValueWriter} perform
   */
  @Benchmark
  public void writeShredded(Blackhole bh) {
    for (int i = 0; i < ITERATIONS; i++) {
      VariantValueWriter.write(noopConsumer, shreddedSchema, preBuiltVariant);
      bh.consume(noopConsumer);
    }
  }

  /**
   * Write the pre-built variant to an unshredded schema (metadata + value only).
   * This is the baseline: the entire variant is written as a single binary blob.
   * Compare with {@link #writeShredded} to see the cost of shredding.
   */
  @Benchmark
  public void writeUnshredded(Blackhole bh) {
    for (int i = 0; i < ITERATIONS; i++) {
      VariantValueWriter.write(noopConsumer, unshreddedSchema, preBuiltVariant);
      bh.consume(noopConsumer);
    }
  }

  // ------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------

  /**
   * Build a complete Variant object from the pre-decided field types. This is the core logic shared
   * between {@link #benchmarkBuildVariant} and setup..
   */
  private Variant buildVariant() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder ob = builder.startObject();

    for (int i = 0; i < fieldCount; i++) {
      ob.appendKey(fieldNames.get(i));
      appendFieldValue(ob, i);
    }

    builder.endObject();
    return builder.build();
  }

  /**
   * Append the value for field {@code i} to {@code ob} according to its type, building nested objects on demand.
   */
  private void appendFieldValue(VariantObjectBuilder ob, int i) {
    final FieldEntry entry = fieldValues[i];
    // special handling of nested.
    if (entry.type == FieldType.Nested) {
      if (depth == Depth.Nested && stringFieldCount > 0) {
        appendNestedObject(ob, i);
      } else {
        // outlier.
        ob.appendNull();
      }
    } else {
      entry.type.append(entry.value, ob);
    }
  }

  /**
   * Append a nested sub-object with {@link #NESTED_FIELD_COUNT} string fields. Field names are
   * drawn from the set of top-level string fields so the nested dictionary overlaps with the parent.
   */
  private void appendNestedObject(VariantObjectBuilder parentOb, int parentIndex) {
    // VariantObjectBuilder does not expose startObject() for nesting directly;
    // build the nested variant separately and embed it as an encoded value.
    VariantBuilder nestedBuilder = new VariantBuilder();
    VariantObjectBuilder nestedOb = nestedBuilder.startObject();

    for (int j = 0; j < NESTED_FIELD_COUNT; j++) {
      int nameIdx = stringFieldIndices[random.nextInt(stringFieldCount)];
      nestedOb.appendKey(fieldNames.get(nameIdx));
      nestedOb.appendString("nested_" + parentIndex + "_" + j);
    }

    nestedBuilder.endObject();
    Variant nested = nestedBuilder.build();
    // embed the nested value buffer directly
    parentOb.appendEncodedValue(nested.getValueBuffer());
  }

  /**
   * Build a shredded schema with typed_value columns matching each field's type.
   * For nested fields, the typed_value is an object group with string sub-fields.
   */
  private GroupType buildShreddedSchema() {
    Types.GroupBuilder<GroupType> typedValueBuilder = Types.optionalGroup();

    for (int i = 0; i < fieldCount; i++) {
      FieldEntry entry = fieldValues[i];
      // Each field in typed_value is a variant group: optional value + optional typed_value
      Types.GroupBuilder<GroupType> fieldBuilder = Types.optionalGroup();
      fieldBuilder.optional(PrimitiveTypeName.BINARY).named("value");
      entry.type.addTypedValue(fieldBuilder);
      typedValueBuilder.addField(fieldBuilder.named(fieldNames.get(i)));
    }

    return Types.buildGroup(Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(typedValueBuilder.named("typed_value"))
        .named("variant_field");
  }

  /**
   * A {@link RecordConsumer} that discards all output, used to isolate shredding cost
   * from actual Parquet encoding and I/O.
   */
  private static final class NoopRecordConsumer extends RecordConsumer {
    @Override
    public void startMessage() {}

    @Override
    public void endMessage() {}

    @Override
    public void startField(String field, int index) {}

    @Override
    public void endField(String field, int index) {}

    @Override
    public void startGroup() {}

    @Override
    public void endGroup() {}

    @Override
    public void addInteger(int value) {}

    @Override
    public void addLong(long value) {}

    @Override
    public void addBoolean(boolean value) {}

    @Override
    public void addBinary(Binary value) {}

    @Override
    public void addFloat(float value) {}

    @Override
    public void addDouble(double value) {}
  }
}
