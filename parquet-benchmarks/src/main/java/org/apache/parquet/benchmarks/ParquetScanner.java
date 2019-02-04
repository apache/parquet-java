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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.benchmarks.BenchmarkFiles.configuration;

/**
 * A simple Parquet scanner that reads specific row blocks and columns
 * This simulates a thread behavior under typical data-parallel computing.
 *
 * @author Takeshi Yoshimura
 *
 */
public class ParquetScanner extends Thread {
  ParquetFileReader reader;
  NopConverter nop;
  Blackhole blackhole;

  public ParquetScanner(Path f, FileMetaData meta, List<BlockMetaData> blocks,
                        List<ColumnDescriptor> columns, Blackhole blackhole) throws IOException {
    // TODO: deprecated, but do we have any other optimal ways?
    this.reader = new ParquetFileReader(configuration, meta, f, blocks, columns);
    this.nop = new NopConverter();
    this.blackhole = blackhole;
  }

  public void scan() throws IOException {
    FileMetaData meta = reader.getFileMetaData();
    MessageType schema = meta.getSchema();
    PageReadStore store;

    while ((store = reader.readNextRowGroup()) != null) {
      ColumnReadStore cs = new ColumnReadStoreImpl(store, nop, schema, meta.getCreatedBy());

      for (ColumnDescriptor column: schema.getColumns()) {
        ColumnReader cr = cs.getColumnReader(column);

        for (long i = 0, e = cr.getTotalValueCount(); i < e; ++i) {
          if (cr.getCurrentDefinitionLevel() == column.getMaxDefinitionLevel()) {
            switch (column.getType()) {
              case BINARY:  blackhole.consume(cr.getBinary()); break;
              case BOOLEAN: blackhole.consume(cr.getBoolean()); break;
              case DOUBLE:  blackhole.consume(cr.getDouble()); break;
              case FLOAT:   blackhole.consume(cr.getFloat()); break;
              case INT32:   blackhole.consume(cr.getInteger()); break;
              case INT64:   blackhole.consume(cr.getLong()); break;
              case INT96:   blackhole.consume(cr.getBinary()); break;
              case FIXED_LEN_BYTE_ARRAY: blackhole.consume(cr.getBinary()); break;
            }
          }
          cr.consume();
        }
      }
    }
    reader.close();
  }

  class NopConverter extends GroupConverter {
    NopPrimitiveConverter c = new NopPrimitiveConverter();
    @Override public void start() { }
    @Override public void end() { }
    @Override public Converter getConverter(int fieldIndex) { return c; }
  }

  class NopPrimitiveConverter extends PrimitiveConverter {
    @Override public GroupConverter asGroupConverter() { return new NopConverter(); }
  }

  @Override
  public void run() {
    try {
      scan();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
