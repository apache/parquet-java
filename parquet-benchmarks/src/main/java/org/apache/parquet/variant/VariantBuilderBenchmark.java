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
package org.apache.parquet.variant;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>{@link #depth} – {@code Flat} (primitives only) or {@code Nested} (some fields are
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
 *   ./parquet-benchmarks/run.sh all org.apache.parquet.variant.VariantBuilderBenchmark \
 *       -wi 5 -i 5 -f 1 -rff target/results.json
 * </pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 100)
@Measurement(iterations = 250)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantBuilderBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(VariantBuilderBenchmark.class);

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
  private static final int ITERATIONS = 1000;

  /** Number of rows written per file in the file-based write/read benchmarks. */
  private static final int FILE_ROWS = ITERATIONS;

  /** Total number of top-level fields in each variant object. */
  @Param({"200" /*, "1000"*/})
  private int fieldCount;

  /** Whether to include nested variant objects as some of the field values. */
  @Param
  private Depth depth;

  /**
   * A counter of strings created; used to ensure limited uniqueness in strings.
   * Reset to 0 in {@link #setupTrial()} so each trial is reproducible.
   */
  private static int counter;

  /**
   * Get a count value.
   * @return a new value.
   */
  private static int count() {
    int c = counter++;
    if (c >= 512) {
      counter = 0;
      c = counter;
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

  /**
   * How many string fields were generated.
   */
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

  /** Pre-written shredded Parquet file bytes, used by {@link #readFileShredded}. */
  private byte[] shreddedFileBytes;

  /** Pre-written unshredded Parquet file bytes, used by {@link #readFileUnshredded}. */
  private byte[] unshreddedFileBytes;

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
    counter = 0;

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
      int typeIndex = random.nextInt(typeCount + 4);
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
          fieldEntry = new FieldEntry(FieldType.BigDecimal, BigDecimal.valueOf(random.nextInt()));
          break;
        case 9:
          fieldEntry = new FieldEntry(FieldType.UUID, UUID.randomUUID());
          break;
        case 10:
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

    // --- pre-written Parquet files for file-based read benchmarks ---
    try {
      shreddedFileBytes = writeVariantsToMemory(shreddedSchema);
      unshreddedFileBytes = writeVariantsToMemory(unshreddedSchema);
    } catch (IOException e) {
      throw new RuntimeException("Failed to pre-write variant files", e);
    }
  }

  // ------------------------------------------------------------------
  // Benchmark methods
  // ------------------------------------------------------------------

  /**
   * Create a {@link VariantBuilder} from scratch, append all fields, call {@link
   * VariantBuilder#build()}. Measures object construction including dictionary encoding.
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void buildVariant(Blackhole bh) {
    Variant v = buildVariant();
    bh.consume(v.getValueBuffer());
    bh.consume(v.getMetadataBuffer());
  }

  /**
   * Serialize-only: re-serializes the pre-built variant value buffer. Isolates the cost of
   * extracting the encoded bytes from a finished Variant without paying for construction.
   *
   * <p>Because {@link Variant#getValueBuffer()} returns the existing buffer, this benchmark
   * primarily measures the ByteBuffer access and the Blackhole overhead..
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void serializeVariant(Blackhole bh) {
    // duplicate() gives an independent position/limit on the same backing array –
    // equivalent to the Iceberg benchmark's outputBuffer.clear() + writeTo() pattern.
    ByteBuffer value = preBuiltVariant.getValueBuffer().duplicate();
    ByteBuffer meta = preBuiltVariant.getMetadataBuffer().duplicate();
    bh.consume(value);
    bh.consume(meta);
  }

  /**
   * Read path: iterate all fields of the pre-built variant, extracting each value. This exercises
   * the field-name lookup and type dispatch that a query engine performs on every row. Nested
   * objects are recursively traversed so that {@code depth=Nested} incurs the full deserialization
   * cost of sub-objects.
   * @param blackhole black hole.
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void deserializeVariant(Blackhole blackhole) {
    deserializeAndConsume(preBuiltVariant, blackhole);
  }

  /**
   * Shred the pre-built variant into a fully typed schema. Measures the cost of type dispatch,
   * field matching, and recursive decomposition that {@link VariantValueWriter} perform
   * @param blackhole black hole.
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void consumeRecordsShredded(Blackhole blackhole) {
    VariantValueWriter.write(noopConsumer, shreddedSchema, preBuiltVariant);
    blackhole.consume(noopConsumer);
  }

  /**
   * Write {@link #FILE_ROWS} rows of the pre-built variant to an in-memory Parquet file using the
   * shredded schema. Measures end-to-end Parquet encoding cost including page/row-group framing.
   * Compare with {@link #consumeRecordsShredded} to quantify the overhead over raw schema traversal.
   * @param blackhole black hole.
   */
  @Benchmark
  public void writeToMemoryFile(Blackhole blackhole) throws IOException {
    writeToMemory(blackhole, shreddedSchema);
  }

  /**
   * Write the pre-built variant to an unshredded schema (metadata + value only).
   * This is the baseline: the entire variant is written as a single binary blob.
   * Compare with {@link #consumeRecordsShredded} to see the cost of shredding.
   * @param blackhole black hole.
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void consumeRecordsUnshredded(Blackhole blackhole) {
    VariantValueWriter.write(noopConsumer, unshreddedSchema, preBuiltVariant);
    blackhole.consume(noopConsumer);
  }

  /**
   * Write {@link #FILE_ROWS} rows of the pre-built variant to an in-memory Parquet file using the
   * unshredded schema (metadata + value binary blobs only). Baseline for {@link #writeToMemoryFile}.
   * @param blackhole black hole.
   */
  @Benchmark
  public void writeToMemoryUnshredded(Blackhole blackhole) throws IOException {
    writeToMemory(blackhole, unshreddedSchema);
  }

  /**
   * Read all rows from the pre-written shredded Parquet file in memory. Measures full Parquet
   * decode cost including typed column decoding and Variant reassembly.
   * @param blackhole black hole.
   * @throws IOException IO failure.
   */
  @Benchmark
  public void readFileShredded(Blackhole blackhole) throws IOException {
    final ByteArrayInputFile inputFile = new ByteArrayInputFile(shreddedFileBytes);
    consumeInputFile(blackhole, inputFile);
  }

  /**
   * Read all rows from the pre-written unshredded Parquet file in memory. Baseline for
   * {@link #readFileShredded}: measures raw binary blob read with no typed column decoding.
   * @param blackhole black hole.
   * @throws IOException IO failure.
   */
  @Benchmark
  public void readFileUnshredded(Blackhole blackhole) throws IOException {
    consumeInputFile(blackhole, new ByteArrayInputFile(unshreddedFileBytes));
  }

  // ------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------

  /**
   * Build a complete Variant object from the pre-decided field types.
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
   * @param ob object
   * @param index index
   */
  private void appendFieldValue(VariantObjectBuilder ob, int index) {
    final FieldEntry entry = fieldValues[index];
    // special handling of nested.
    if (entry.type == FieldType.Nested) {
      if (depth == Depth.Nested && stringFieldCount > 0) {
        appendNestedObject(ob, index);
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
   *
   * @param parentOb parent object.
   * @param parentIndex parent index.
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
   * @return the group type for a shredded object.
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
   *  Recursively deserialize a variant object, descending into any nested objects.
   *
   * @param variant variant to deserialize.
   * @param blackhole black hole.
   */
  private void deserializeAndConsume(Variant variant, Blackhole blackhole) {
    int n = variant.numObjectElements();
    for (int i = 0; i < n; i++) {
      Variant.ObjectField field = variant.getFieldAtIndex(i);
      blackhole.consume(field.key);
      if (field.value.getType() == Variant.Type.OBJECT) {
        deserializeAndConsume(field.value, blackhole);
      } else {
        blackhole.consume(field.value.getValueBuffer());
      }
    }
  }

  /**
   * Write {@link #FILE_ROWS} copies of {@link #preBuiltVariant} to a fresh in-memory Parquet file
   * using the given schema. Used both in {@link #setupTrial()} to pre-build read buffers and as the
   * body of the write-file benchmarks.
   *
   * @param schema group schema.
   * @return the byte of an in-memory parquet file.
   * @throws IOException IO failure.
   */
  private byte[] writeVariantsToMemory(GroupType schema) throws IOException {
    ByteArrayOutputFile out = new ByteArrayOutputFile();
    try (ParquetWriter<Variant> writer = new VariantWriterBuilder(out, schema).build()) {
      for (int i = 0; i < FILE_ROWS; i++) {
        writer.write(preBuiltVariant);
      }
    }
    LOG.info("Written Parquet file has size: {}", out.size());
    return out.toByteArray();
  }

  /**
   * Write the prebuilt variant to a memory output stream.
   * As the same variant is written, compression on the shredded variant should be good.
   * @param blackhole black hole
   * @param schema schema
   * @throws IOException write failure
   */
  private void writeToMemory(final Blackhole blackhole, final GroupType schema) throws IOException {
    blackhole.consume(writeVariantsToMemory(schema));
  }

  /**
   * Consume an Input file.
   * @param blackhole black hole
   * @param inputFile input file
   * @throws IOException IO failure.
   */
  private static void consumeInputFile(final Blackhole blackhole, final ByteArrayInputFile inputFile)
      throws IOException {
    try (ParquetReader<Variant> reader = new VariantReaderBuilder(inputFile).build()) {
      Variant v;
      while ((v = reader.read()) != null) {
        blackhole.consume(v);
      }
    }
  }

  // ------------------------------------------------------------------
  // In-memory I/O
  // ------------------------------------------------------------------

  /** An {@link OutputFile} backed by a {@link ByteArrayOutputStream}. */
  private static final class ByteArrayOutputFile implements OutputFile {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] toByteArray() {
      return baos.toByteArray();
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return newStream();
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return newStream();
    }

    private PositionOutputStream newStream() {
      return new PositionOutputStream() {
        private long pos = 0;

        @Override
        public void write(int b) throws IOException {
          baos.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          baos.write(b, off, len);
          pos += len;
        }

        @Override
        public long getPos() {
          return pos;
        }
      };
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }

    int size() {
      return baos.size();
    }
  }

  /** An {@link InputFile} backed by a {@code byte[]}. */
  private static final class ByteArrayInputFile implements InputFile {
    private final byte[] data;

    ByteArrayInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new SeekableInputStream() {
        private int pos = 0;

        @Override
        public int read() {
          return pos < data.length ? (data[pos++] & 0xFF) : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
          int remaining = data.length - pos;
          if (remaining <= 0) return -1;
          int n = Math.min(len, remaining);
          System.arraycopy(data, pos, b, off, n);
          pos += n;
          return n;
        }

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void seek(long newPos) {
          pos = (int) newPos;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
          readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
          if (pos + len > data.length) {
            throw new EOFException("Unexpected end of data");
          }
          System.arraycopy(data, pos, bytes, start, len);
          pos += len;
        }

        @Override
        public int read(ByteBuffer buf) {
          int len = buf.remaining();
          int remaining = data.length - pos;
          if (remaining <= 0) return -1;
          int n = Math.min(len, remaining);
          buf.put(data, pos, n);
          pos += n;
          return n;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
          int len = buf.remaining();
          if (pos + len > data.length) {
            throw new IOException("Unexpected end of data");
          }
          buf.put(data, pos, len);
          pos += len;
        }

        @Override
        public void close() {}
      };
    }
  }

  // ------------------------------------------------------------------
  // Write support
  // ------------------------------------------------------------------

  /**
   * {@link ParquetWriter.Builder} for {@link Variant} records using {@link VariantWriteSupport}.
   */
  private static final class VariantWriterBuilder extends ParquetWriter.Builder<Variant, VariantWriterBuilder> {
    private final GroupType variantGroup;

    VariantWriterBuilder(OutputFile file, GroupType variantGroup) {
      super(file);
      this.variantGroup = variantGroup;
    }

    @Override
    protected VariantWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<Variant> getWriteSupport(Configuration conf) {
      return new VariantWriteSupport(variantGroup);
    }
  }

  /**
   * {@link WriteSupport} that writes a single {@link Variant} field named {@code "variant_field"}
   * per message using {@link VariantValueWriter}.
   */
  private static final class VariantWriteSupport extends WriteSupport<Variant> {
    private static final String FIELD_NAME = "variant_field";
    private final GroupType variantGroup;
    private RecordConsumer consumer;

    VariantWriteSupport(GroupType variantGroup) {
      this.variantGroup = variantGroup;
    }

    @Override
    public WriteContext init(Configuration conf) {
      return new WriteContext(new MessageType("message", variantGroup), Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.consumer = recordConsumer;
    }

    @Override
    public void write(Variant record) {
      consumer.startMessage();
      consumer.startField(FIELD_NAME, 0);
      VariantValueWriter.write(consumer, variantGroup, record);
      consumer.endField(FIELD_NAME, 0);
      consumer.endMessage();
    }
  }

  // ------------------------------------------------------------------
  // Read support
  // ------------------------------------------------------------------

  /**
   * {@link ParquetReader.Builder} for {@link Variant} records using {@link VariantReadSupport}.
   */
  private static final class VariantReaderBuilder extends ParquetReader.Builder<Variant> {
    VariantReaderBuilder(InputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<Variant> getReadSupport() {
      return new VariantReadSupport();
    }
  }

  /**
   * {@link ReadSupport} that materializes each row as a {@link Variant} using
   * {@link VariantConverters}.
   */
  private static final class VariantReadSupport extends ReadSupport<Variant> {
    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<Variant> prepareForRead(
        Configuration conf,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadContext readContext) {
      GroupType variantGroup = fileSchema.getType("variant_field").asGroupType();
      return new VariantRecordMaterializer(fileSchema, variantGroup);
    }
  }

  /** Materializes a single {@link Variant} from a Parquet message containing one variant field. */
  private static final class VariantRecordMaterializer extends RecordMaterializer<Variant> {
    private final MessageGroupConverter root;

    VariantRecordMaterializer(MessageType messageType, GroupType variantGroup) {
      this.root = new MessageGroupConverter(variantGroup);
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }

    @Override
    public Variant getCurrentRecord() {
      return root.getCurrentVariant();
    }
  }

  /**
   * Top-level (message) {@link GroupConverter} that delegates field 0 ({@code "variant_field"})
   * to a {@link VariantGroupConverter}.
   */
  private static final class MessageGroupConverter extends GroupConverter {
    private final VariantGroupConverter variantConverter;

    MessageGroupConverter(GroupType variantGroup) {
      this.variantConverter = new VariantGroupConverter(variantGroup);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return variantConverter;
    }

    @Override
    public void start() {}

    @Override
    public void end() {}

    Variant getCurrentVariant() {
      return variantConverter.getCurrentVariant();
    }
  }

  /**
   * {@link GroupConverter} for a variant group. Implements
   * {@link VariantConverters.ParentConverter} so it can be used with
   * {@link VariantConverters#newVariantConverter}.
   */
  private static final class VariantGroupConverter extends GroupConverter
      implements VariantConverters.ParentConverter<VariantBuilder> {
    private final GroupConverter wrapped;
    private VariantBuilder builder;
    private ImmutableMetadata metadata;
    private Variant currentVariant;

    VariantGroupConverter(GroupType variantGroup) {
      this.wrapped = VariantConverters.newVariantConverter(variantGroup, this::setMetadata, this);
    }

    private void setMetadata(ByteBuffer buf) {
      this.metadata = new ImmutableMetadata(buf);
    }

    @Override
    public void build(Consumer<VariantBuilder> consumer) {
      if (builder == null) {
        builder = new VariantBuilder(metadata);
      }
      consumer.accept(builder);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return wrapped.getConverter(fieldIndex);
    }

    @Override
    public void start() {
      builder = null;
      metadata = null;
      wrapped.start();
    }

    @Override
    public void end() {
      wrapped.end();
      if (builder == null) {
        builder = new VariantBuilder(metadata);
      }
      builder.appendNullIfEmpty();
      currentVariant = builder.build();
    }

    Variant getCurrentVariant() {
      return currentVariant;
    }
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
