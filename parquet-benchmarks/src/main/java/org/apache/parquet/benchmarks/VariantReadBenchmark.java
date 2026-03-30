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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
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
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(MILLISECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(VariantReadBenchmark.class);

  /** Number of rows written per file. */
  private static final int NUM_ROWS = 1000_000;

  /** 20 col4 values, one per category. */
  private static final String[] COL4_VALUES;

  static {
    COL4_VALUES = new String[20];
    for (int i = 0; i < 20; i++) {
      COL4_VALUES[i] = "col4_category_" + i;
    }
  }

  private MessageType unshreddedSchema;

  private MessageType shreddedSchema;
  private Configuration conf;
  private FileSystem fs;
  private Path shreddedFile;
  private Path unshreddedFile;

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
        + "    }"
        + "    required group varid {"
        + "      optional binary value;"
        + "      optional int64 typed_value;"
        + "    }"
        + "    required group varcategory {"
        + "      optional binary value;"
        + "      optional int32 typed_value;"
        + "    }"
        + "    required group col4 {"
        + "      optional binary value;"
        + "      optional binary typed_value (STRING);"
        + "    }"
        + "  }"
        + "}" +
        "}");
  }

  @Setup(Level.Trial)
  public void setupBenchmarks() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    cleanup();
    fs.mkdirs(BenchmarkFiles.targetDir);
    shreddedFile = new Path(BenchmarkFiles.targetDir, "shredded.parquet");
    unshreddedFile = new Path(BenchmarkFiles.targetDir, "unshredded.parquet");
    writeDataset(unshreddedSchema, unshreddedFile);
    writeDataset(shreddedSchema, shreddedFile);
  }

  private void cleanup() throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(BenchmarkFiles.targetDir, true);
  }

  @Benchmark
  public void unshreddedSchemaFile(Blackhole blackhole) throws IOException {
    fs.delete(unshreddedFile, false);
    final FileStatus st = fs.getFileStatus(unshreddedFile);
    LOG.info("Unshredded file size {}", st.getLen());
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
    LOG.info("Size of file {} size {}", path, st.getLen());}

  @Benchmark
  public void readShreddedSchemaFile(Blackhole blackhole) throws IOException {

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
}
