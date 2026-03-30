/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.benchmarks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantConverters;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 *   id: int64 -> unique per row
 *   category: int32 -> in range 0-19:  (fileNum % 2) * 10 + (id % 10);
 *   nested: variant
 *       .idstr: string -> unique string per row
 *       .varid: int64  -> id
 *       .varcategory: int32  -> category (0-19)
 *       .col4: string -> non-unique string per row (picked from 20 values based on category)
 * </pre>
 * <p>Build and run:
 *
 * <pre>
 *   ./mvnw --projects parquet-benchmarks -amd -DskipTests -Denforcer.skip=true clean package
 *   ./parquet-benchmarks/run.sh all org.apache.parquet.benchmarks.VariantReadBenchmark \
 *       -wi 5 -i 5 -f 1 -rff target/results.json
 * </pre>
 * *
 */
@Fork(0)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(MILLISECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(VariantReadBenchmark.class);

  /** Number of rows written per file. */
  private static final int NUM_ROWS = 10_000_000;

  /** 20 col4 values, one per category. */
  private static final String[] COL4_VALUES;

  static {
    COL4_VALUES = new String[20];
    for (int i = 0; i < 20; i++) {
      COL4_VALUES[i] = "col4_category_" + i;
    }
  }

  @Param({"true", "false"})
  public boolean shredded;

  private final MessageType unshreddedSchema;
  private final MessageType shreddedSchema;
  private final MessageType projectionSchema;
  private MessageType activeSchema;
  private Configuration conf;
  private FileSystem fs;
  private Path shreddedFile;
  private Path unshreddedFile;
  private Path dataFile;

  public VariantReadBenchmark() {
    unshreddedSchema = parseMessageType("message vschema { "
        + "required int64 id;"
        + "required int32 category;"
        + "optional group nested (VARIANT(1)) {"
        + "  required binary metadata;"
        + "  required binary value;"
        + "  }"
        + "}");

    shreddedSchema = parseMessageType("message vschema { "
        + "required int64 id;"
        + "required int32 category;"
        + "optional group nested (VARIANT(1)) {"
        + "  required binary metadata;"
        + "  optional binary value;"
        + "  optional group typed_value {"
        + "    required group idstr {"
        + "      optional binary value;"
        + "      optional binary typed_value (STRING);"
        + "      }"
        + "    required group varid {"
        + "      optional binary value;"
        + "      optional int64 typed_value;"
        + "      }"
        + "    required group varcategory {"
        + "      optional binary value;"
        + "      optional int32 typed_value;"
        + "      }"
        + "    required group col4 {"
        + "      optional binary value;"
        + "      optional binary typed_value (STRING);"
        + "      }"
        + "    }"
        + "   }"
        + "}");

    projectionSchema = parseMessageType("message vschema { "
        + "required int64 id;"
        + "required int32 category;"
        + "optional group nested (VARIANT(1)) {"
        + "  required binary metadata;"
        + "  optional binary value;"
        + "  optional group typed_value {"
        + "    required group varcategory {"
        + "      optional binary value;"
        + "      optional int32 typed_value;"
        + "      }"
        + "   }"
        + "}");

  }

  @Setup(Level.Trial)
  public void setupBenchmarks() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    cleanup();
    fs.mkdirs(BenchmarkFiles.targetDir);
    // using different filenames assists with manual examination
    // of the contents.
    shreddedFile = new Path(BenchmarkFiles.targetDir, "shredded.parquet");
    unshreddedFile = new Path(BenchmarkFiles.targetDir, "unshredded.parquet");
    if (shredded) {
      dataFile = shreddedFile;
      activeSchema = shreddedSchema;
    } else {
      dataFile = unshreddedFile;
      activeSchema = unshreddedSchema;
    }
    writeDataset(activeSchema, dataFile);
  }

  private void cleanup() throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(BenchmarkFiles.targetDir, true);
  }

  private void writeDataset(final MessageType schema, final Path path) throws IOException {
    GroupType nestedGroup = schema.getType("nested").asGroupType();
    try (ParquetWriter<RowRecord> writer =
        new RowWriterBuilder(HadoopOutputFile.fromPath(path, conf), schema, nestedGroup).build()) {
      for (int i = 0; i < NUM_ROWS; i++) {
        int category = i % 20;
        writer.write(new RowRecord(i, category, buildVariant(i, category, COL4_VALUES[category])));
      }
    }
    final FileStatus st = fs.getFileStatus(path);
    LOG.info("Size of file {} size {}", path, st.getLen());
  }

  /**
   * Reads the records, reconstructing the full record from the variant.
   * @param blackhole black hole.
   */
  @Benchmark
  public void readFileWithoutProjection(Blackhole blackhole) throws IOException {
    try (ParquetReader<RowRecord> reader = new RowReaderBuilder(HadoopInputFile.fromPath(dataFile, conf)).build()) {
      RowRecord row;
      while ((row = reader.read()) != null) {
        Variant varcategory = row.variant.getFieldByKey("varcategory");
        if (varcategory != null) {
          blackhole.consume(varcategory.getInt());
        }
      }
    }
  }

  /**
   * Like {@link #readFileWithoutProjection(Blackhole)}, but uses column projection to read only the
   * {@code nested.typed_value.varcategory} column, skipping {@code id}, {@code category},
   * {@code idstr}, {@code varid}, and {@code col4}.
   */
  @Benchmark
  public void readFileProjected(Blackhole blackhole) throws IOException {
    try (ParquetReader<Variant> reader =
        new ProjectedReaderBuilder(HadoopInputFile.fromPath(dataFile, conf)).build()) {
      Variant nested;
      while ((nested = reader.read()) != null) {
        Variant varcategory = nested.getFieldByKey("varcategory");
        if (varcategory != null) {
          blackhole.consume(varcategory.getInt());
        }
      }
    }
  }

  /**
   * Build the nested variant structure.
   *
   * @param id row ID
   * @param category category
   * @param col4 string for column 4
   *
   * @return a variant
   */
  private static Variant buildVariant(long id, int category, String col4) {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder obj = builder.startObject();
    obj.appendKey("idstr");
    obj.appendString("item_" + id);
    obj.appendKey("varid");
    obj.appendLong(id);
    obj.appendKey("varcategory");
    obj.appendInt(category);
    obj.appendKey("col4");
    obj.appendString(col4);
    builder.endObject();
    return builder.build();
  }

  // ------------------------------------------------------------------
  // Row record
  // ------------------------------------------------------------------

  /** A single row: integer id and category columns plus a nested variant. */
  private static final class RowRecord {
    final long id;
    final int category;
    final Variant variant;

    RowRecord(long id, int category, Variant variant) {
      this.id = id;
      this.category = category;
      this.variant = variant;
    }
  }

  // ------------------------------------------------------------------
  // Write support
  // ------------------------------------------------------------------

  /** {@link ParquetWriter.Builder} for {@link RowRecord} values. */
  private static final class RowWriterBuilder extends ParquetWriter.Builder<RowRecord, RowWriterBuilder> {
    private final MessageType schema;
    private final GroupType nestedGroup;

    RowWriterBuilder(OutputFile file, MessageType schema, GroupType nestedGroup) {
      super(file);
      this.schema = schema;
      this.nestedGroup = nestedGroup;
    }

    @Override
    protected RowWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<RowRecord> getWriteSupport(Configuration conf) {
      return new RowWriteSupport(schema, nestedGroup);
    }
  }

  /**
   * {@link WriteSupport} that writes {@code id} (INT64), {@code category} (INT32), and
   * {@code nested} (VARIANT group) fields for each {@link RowRecord}.
   */
  private static final class RowWriteSupport extends WriteSupport<RowRecord> {
    private final MessageType schema;
    private final GroupType nestedGroup;
    private RecordConsumer consumer;

    RowWriteSupport(MessageType schema, GroupType nestedGroup) {
      this.schema = schema;
      this.nestedGroup = nestedGroup;
    }

    @Override
    public WriteContext init(Configuration conf) {
      return new WriteContext(schema, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.consumer = recordConsumer;
    }

    @Override
    public void write(RowRecord record) {
      consumer.startMessage();
      consumer.startField("id", 0);
      consumer.addLong(record.id);
      consumer.endField("id", 0);
      consumer.startField("category", 1);
      consumer.addInteger(record.category);
      consumer.endField("category", 1);
      consumer.startField("nested", 2);
      VariantValueWriter.write(consumer, nestedGroup, record.variant);
      consumer.endField("nested", 2);
      consumer.endMessage();
    }
  }

  // ------------------------------------------------------------------
  // Read support
  // ------------------------------------------------------------------

  /** {@link ParquetReader.Builder} for {@link RowRecord} values. */
  private static final class RowReaderBuilder extends ParquetReader.Builder<RowRecord> {
    RowReaderBuilder(InputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<RowRecord> getReadSupport() {
      return new RowReadSupport();
    }
  }

  /** {@link ReadSupport} that materializes each row as a {@link RowRecord}. */
  private static final class RowReadSupport extends ReadSupport<RowRecord> {
    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<RowRecord> prepareForRead(
        Configuration conf,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadContext readContext) {
      GroupType nestedGroup = fileSchema.getType("nested").asGroupType();
      return new RowRecordMaterializer(fileSchema, nestedGroup);
    }
  }

  /** Materializes a {@link RowRecord} from a 3-field Parquet message. */
  private static final class RowRecordMaterializer extends RecordMaterializer<RowRecord> {
    private final RowMessageConverter root;

    RowRecordMaterializer(MessageType messageType, GroupType nestedGroup) {
      this.root = new RowMessageConverter(nestedGroup);
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }

    @Override
    public RowRecord getCurrentRecord() {
      return root.getCurrentRecord();
    }
  }

  /**
   * Root {@link GroupConverter} for a message with {@code id} (field 0), {@code category} (field
   * 1), and {@code nested} variant (field 2).
   */
  private static final class RowMessageConverter extends GroupConverter {
    private final PrimitiveConverter idConverter;
    private final PrimitiveConverter categoryConverter;
    private final RowVariantGroupConverter variantConverter;
    private long id;
    private int category;

    RowMessageConverter(GroupType nestedGroup) {
      idConverter = new PrimitiveConverter() {
        @Override
        public void addLong(long value) {
          id = value;
        }
      };
      categoryConverter = new PrimitiveConverter() {
        @Override
        public void addInt(int value) {
          category = value;
        }
      };
      variantConverter = new RowVariantGroupConverter(nestedGroup);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      switch (fieldIndex) {
        case 0:
          return idConverter;
        case 1:
          return categoryConverter;
        case 2:
          return variantConverter;
        default:
          throw new IllegalArgumentException("Unknown field index: " + fieldIndex);
      }
    }

    @Override
    public void start() {}

    @Override
    public void end() {}

    RowRecord getCurrentRecord() {
      return new RowRecord(id, category, variantConverter.getCurrentVariant());
    }
  }

  /**
   * {@link GroupConverter} for the {@code nested} variant field. Implements
   * {@link VariantConverters.ParentConverter} so it works with
   * {@link VariantConverters#newVariantConverter}.
   */
  private static final class RowVariantGroupConverter extends GroupConverter
      implements VariantConverters.ParentConverter<VariantBuilder> {
    private final GroupConverter wrapped;
    private VariantBuilder builder;
    private ImmutableMetadata metadata;
    private Variant currentVariant;

    RowVariantGroupConverter(GroupType variantGroup) {
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

  // ------------------------------------------------------------------
  // Projected read support (varcategory only)
  // ------------------------------------------------------------------

  /** {@link ParquetReader.Builder} using {@link ProjectedReadSupport}. */
  private static final class ProjectedReaderBuilder extends ParquetReader.Builder<Variant> {
    ProjectedReaderBuilder(InputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<Variant> getReadSupport() {
      return new ProjectedReadSupport();
    }
  }

  /**
   * {@link ReadSupport} that projects the file schema down to only the {@code nested} variant's
   * {@code varcategory} field, skipping {@code id}, {@code category}, {@code idstr},
   * {@code varid}, and {@code col4} column chunks entirely.
   */
  private static final class ProjectedReadSupport extends ReadSupport<Variant> {
    private static final MessageType VARCATEGORY_PROJECTION = new MessageType(
        "vschema",
        Types.optionalGroup()
            .as(LogicalTypeAnnotation.variantType((byte) 1))
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(Types.optionalGroup()
                .addField(Types.optionalGroup()
                    .optional(PrimitiveTypeName.BINARY)
                    .named("value")
                    .optional(PrimitiveTypeName.INT32)
                    .named("typed_value")
                    .named("varcategory"))
                .named("typed_value"))
            .named("nested"));

    @Override
    public ReadContext init(InitContext context) {
      MessageType fileSchema = context.getFileSchema();
      GroupType nested = fileSchema.getType("nested").asGroupType();
      if (nested.containsField("typed_value")) {
        return new ReadContext(VARCATEGORY_PROJECTION);
      }
      // Unshredded file: projection designed for typed columns provides no benefit and
      // causes schema mismatch overhead — fall back to the full file schema.
      return new ReadContext(fileSchema);
    }

    @Override
    public RecordMaterializer<Variant> prepareForRead(
        Configuration conf,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadContext readContext) {
      // Use the requested schema from the ReadContext — either VARCATEGORY_PROJECTION
      // (shredded) or the full file schema (unshredded fallback).
      GroupType nestedGroup = readContext.getRequestedSchema().getType("nested").asGroupType();
      return new ProjectedRecordMaterializer(nestedGroup);
    }
  }

  /** Materializes the {@code nested} {@link Variant} from the projected single-field schema. */
  private static final class ProjectedRecordMaterializer extends RecordMaterializer<Variant> {
    private final ProjectedMessageConverter root;

    ProjectedRecordMaterializer(GroupType nestedGroup) {
      this.root = new ProjectedMessageConverter(nestedGroup);
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
   * Root converter for the projected schema: single field 0 ({@code nested}). No converters for
   * {@code id} or {@code category} — those columns are not requested and never decoded.
   */
  private static final class ProjectedMessageConverter extends GroupConverter {
    private final RowVariantGroupConverter variantConverter;

    ProjectedMessageConverter(GroupType nestedGroup) {
      this.variantConverter = new RowVariantGroupConverter(nestedGroup);
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
}
