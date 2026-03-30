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
import org.apache.parquet.schema.MessageType;
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
 *   ./parquet-benchmarks/run.sh all org.apache.parquet.benchmarks.VariantProjectionBenchmark \
 *       -wi 5 -i 5 -f 1 -rff target/results.json
 * </pre>
 * *
 */
@Fork(0)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(MILLISECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantProjectionBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(VariantProjectionBenchmark.class);

  /** Number of rows written per file. */
  private static final int NUM_ROWS = 1_000_000;

  /**
   * General specification of the records.
   */
  public static final String UNSHREDDED_SCHEMA = "message vschema { "
      + "required int64 id;"
      + "required int32 category;"
      + "optional group nested (VARIANT(1)) {"
      + "  required binary metadata;"
      + "  required binary value;"
      + "  }"
      + "}";

  /**
   * Detailed specification declaring all variant columns.
   */
  public static final String SHREDDED_SCHEMA = "message vschema { "
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
      + "}";

  /**
   * The select schema is a subset of {@link #SHREDDED_SCHEMA}.
   */
  public static final String SELECT_SCHEMA = "message vschema { "
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
      + "    }"
      + "  }"
      + "}";

  private static final int CATEGORIES = 20;

  /** The col4 values, one per category. */
  private static final String[] COL4_VALUES;

  static {
    COL4_VALUES = new String[CATEGORIES];
    for (int i = 0; i < CATEGORIES; i++) {
      COL4_VALUES[i] = "col4_category_" + i;
    }
  }

  @Param({"true", "false"})
  public boolean shredded;

  /**
   * The record schema with the unshredded variant.
   */
  private final MessageType unshreddedSchema;

  /**
   * The shredded schema splits all expected variant group entries
   * into their own columns.
   */
  private final MessageType shreddedSchema;

  /**
   * Select schema.
   * A subset of the {@link }
   */
  private final MessageType selectSchema;

  private Configuration conf;
  private FileSystem fs;
  private Path dataFile;

  public VariantProjectionBenchmark() {
    // build the schemas.
    // doing this in the constructor makes it slightly easier to debug
    // schema errors.
    unshreddedSchema = parseMessageType(UNSHREDDED_SCHEMA);
    shreddedSchema = parseMessageType(SHREDDED_SCHEMA);
    selectSchema = parseMessageType(SELECT_SCHEMA);
  }

  @Setup(Level.Trial)
  public void setupBenchmarks() throws IOException {
    conf = new Configuration();
    // hadoop 3.4.3 turn off CRC and use direct nio range reads.
    conf.setBoolean("fs.file.checksum.verify", false);
    fs = FileSystem.getLocal(conf);
    cleanup();
    fs.mkdirs(BenchmarkFiles.targetDir);
    // using different filenames assists with manual examination
    // of the contents.
    MessageType activeSchema;
    if (shredded) {
      dataFile = new Path(BenchmarkFiles.targetDir, "shredded.parquet");
      activeSchema = shreddedSchema;
    } else {
      dataFile = new Path(BenchmarkFiles.targetDir, "unshredded.parquet");
      activeSchema = unshreddedSchema;
    }
    fs.delete(dataFile, false);
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
        int category = i % CATEGORIES;
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
  public void readAllRecords(Blackhole blackhole) throws IOException {
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
   * Projected read, using {@link #SELECT_SCHEMA} as the record schema.
   */
  @Benchmark
  public void readProjectedLeanSchema(Blackhole blackhole) throws IOException {
    try (ParquetReader<RowRecord> reader =
        new ProjectedReaderBuilder(HadoopInputFile.fromPath(dataFile, conf), true).build()) {
      consumeProjectedFields(blackhole, reader);
    }
  }

  /**
   * Consume only those fields which are in the projection schema.
   * Other variant columns may or may not be present.
   * @param blackhole black hold.
   * @param reader reader.
   */
  private static void consumeProjectedFields(final Blackhole blackhole, final ParquetReader<RowRecord> reader)
      throws IOException {
    RowRecord row;
    while ((row = reader.read()) != null) {
      blackhole.consume(row.id);
      blackhole.consume(row.category);
      Variant varcategory = row.variant.getFieldByKey("varcategory");
      if (varcategory != null) {
        blackhole.consume(varcategory.getInt());
      }
    }
  }

  /**
   * Read projected with the file schema, not the leaner one.
   */
  @Benchmark
  public void readProjectedFileSchema(Blackhole blackhole) throws IOException {
    try (ParquetReader<RowRecord> reader =
        new ProjectedReaderBuilder(HadoopInputFile.fromPath(dataFile, conf), false).build()) {
      consumeProjectedFields(blackhole, reader);
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

  /** Materializes a {@link RowRecord}. */
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

  /** {@link ParquetReader.Builder} using {@link ProjectedReadSupport}. */
  private final class ProjectedReaderBuilder extends ParquetReader.Builder<RowRecord> {

    /** Read support for this read. */
    private final ProjectedReadSupport readSupport;

    /**
     * Reader for projected reads.
     * @param file input file
     * @param useSelectSchema true if the select schema should be used instead of the file schema.
     */
    ProjectedReaderBuilder(InputFile file, boolean useSelectSchema) {
      super(file);
      this.readSupport = new ProjectedReadSupport(useSelectSchema);
    }

    @Override
    protected ReadSupport<RowRecord> getReadSupport() {
      return readSupport;
    }
  }

  /**
   * {@link ReadSupport} for proejection.
   */
  private final class ProjectedReadSupport extends ReadSupport<RowRecord> {

    /**
     * Use the optimized select schema?
     */
    private final boolean useSelectSchema;

    /**
     * Constructor.
     * @param useSelectSchema Use the optimized select schema?
     */
    public ProjectedReadSupport(final boolean useSelectSchema) {
      this.useSelectSchema = useSelectSchema;
    }

    @Override
    public ReadContext init(InitContext context) {
      MessageType fileSchema = useSelectSchema ? selectSchema : context.getFileSchema();
      return new ReadContext(fileSchema);
    }

    @Override
    public RecordMaterializer<RowRecord> prepareForRead(
        Configuration conf,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadContext readContext) {
      MessageType requestedSchema = readContext.getRequestedSchema();
      GroupType nestedGroup = requestedSchema.getType("nested").asGroupType();
      return new ProjectedRecordMaterializer(requestedSchema, nestedGroup);
    }
  }

  /** Materializes a {@link RowRecord} from the projected schema. */
  private static final class ProjectedRecordMaterializer extends RecordMaterializer<RowRecord> {
    private final ProjectedMessageConverter root;

    ProjectedRecordMaterializer(MessageType requestedSchema, GroupType nestedGroup) {
      this.root = new ProjectedMessageConverter(requestedSchema, nestedGroup);
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
   * Root converter for the projected schema. Routes {@code id}, {@code category}, and {@code
   * nested} to dedicated converters; indices are resolved dynamically from the requested schema so
   * both the shredded projection and the unshredded full-schema fallback work correctly.
   */
  private static final class ProjectedMessageConverter extends GroupConverter {
    private final int idIndex;
    private final int categoryIndex;
    private final int nestedIndex;
    private final PrimitiveConverter idConverter;
    private final PrimitiveConverter categoryConverter;
    private final RowVariantGroupConverter variantConverter;
    private long id;
    private int category;

    ProjectedMessageConverter(MessageType requestedSchema, GroupType nestedGroup) {
      idIndex = requestedSchema.getFieldIndex("id");
      categoryIndex = requestedSchema.getFieldIndex("category");
      nestedIndex = requestedSchema.getFieldIndex("nested");
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
      if (fieldIndex == idIndex) {
        return idConverter;
      } else if (fieldIndex == categoryIndex) {
        return categoryConverter;
      } else if (fieldIndex == nestedIndex) {
        return variantConverter;
      }
      throw new IllegalArgumentException("Unknown field index: " + fieldIndex);
    }

    @Override
    public void start() {
      id = -1;
      category = -1;
    }

    @Override
    public void end() {}

    RowRecord getCurrentRecord() {
      return new RowRecord(id, category, variantConverter.getCurrentVariant());
    }
  }
}
