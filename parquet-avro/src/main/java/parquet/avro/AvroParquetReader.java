/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.avro;

import java.io.IOException;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.ParquetReader;

/**
 * Read Avro records from a Parquet file.
 */
public class AvroParquetReader<T extends IndexedRecord> extends ParquetReader<T> {

  public Builder<T> builder(Path file) {
    return ParquetReader.builder(new AvroReadSupport<T>(), file);
  }

  /**
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  public AvroParquetReader(Path file) throws IOException {
    super(file, new AvroReadSupport<T>());
  }

  /**
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  public AvroParquetReader(Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(file, new AvroReadSupport<T>(), unboundRecordFilter);
  }

  /**
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  public AvroParquetReader(Configuration conf, Path file) throws IOException {
    super(conf, file, new AvroReadSupport<T>());
  }

  /**
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  public AvroParquetReader(Configuration conf, Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(conf, file, new AvroReadSupport<T>(), unboundRecordFilter);
  }
}
