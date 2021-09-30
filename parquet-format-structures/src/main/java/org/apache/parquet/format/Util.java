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

package org.apache.parquet.format;

import static org.apache.parquet.format.FileMetaData._Fields.CREATED_BY;
import static org.apache.parquet.format.FileMetaData._Fields.KEY_VALUE_METADATA;
import static org.apache.parquet.format.FileMetaData._Fields.NUM_ROWS;
import static org.apache.parquet.format.FileMetaData._Fields.ROW_GROUPS;
import static org.apache.parquet.format.FileMetaData._Fields.SCHEMA;
import static org.apache.parquet.format.FileMetaData._Fields.VERSION;
import static org.apache.parquet.format.event.Consumers.fieldConsumer;
import static org.apache.parquet.format.event.Consumers.listElementsOf;
import static org.apache.parquet.format.event.Consumers.listOf;
import static org.apache.parquet.format.event.Consumers.struct;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import org.apache.parquet.format.event.Consumers.Consumer;
import org.apache.parquet.format.event.Consumers.DelegatingFieldConsumer;
import org.apache.parquet.format.event.EventBasedThriftReader;
import org.apache.parquet.format.event.TypedConsumer.I32Consumer;
import org.apache.parquet.format.event.TypedConsumer.I64Consumer;
import org.apache.parquet.format.event.TypedConsumer.StringConsumer;

/**
 * Utility to read/write metadata
 * We use the TCompactProtocol to serialize metadata
 */
public class Util {

  public static void writeColumnIndex(ColumnIndex columnIndex, OutputStream to) throws IOException {
    write(columnIndex, to);
  }

  public static ColumnIndex readColumnIndex(InputStream from) throws IOException {
    return read(from, new ColumnIndex());
  }

  public static void writeOffsetIndex(OffsetIndex offsetIndex, OutputStream to) throws IOException {
    write(offsetIndex, to);
  }

  public static OffsetIndex readOffsetIndex(InputStream from) throws IOException {
    return read(from, new OffsetIndex());
  }

  public static void writePageHeader(PageHeader pageHeader, OutputStream to) throws IOException {
    write(pageHeader, to);
  }

  public static PageHeader readPageHeader(InputStream from) throws IOException {
    return MetadataValidator.validate(read(from, new PageHeader()));
  }

  public static void writeFileMetaData(org.apache.parquet.format.FileMetaData fileMetadata, OutputStream to) throws IOException {
    write(fileMetadata, to);
  }

  public static FileMetaData readFileMetaData(InputStream from) throws IOException {
    return read(from, new FileMetaData());
  }
  /**
   * reads the meta data from the stream
   * @param from the stream to read the metadata from
   * @param skipRowGroups whether row groups should be skipped
   * @return the resulting metadata
   * @throws IOException if any I/O error occurs during the reading
   */
  public static FileMetaData readFileMetaData(InputStream from, boolean skipRowGroups) throws IOException {
    FileMetaData md = new FileMetaData();
    if (skipRowGroups) {
      readFileMetaData(from, new DefaultFileMetaDataConsumer(md), skipRowGroups);
    } else {
      read(from, md);
    }
    return md;
  }

  /**
   * To read metadata in a streaming fashion.
   *
   */
  public static abstract class FileMetaDataConsumer {
    abstract public void setVersion(int version);
    abstract public void setSchema(List<SchemaElement> schema);
    abstract public void setNumRows(long numRows);
    abstract public void addRowGroup(RowGroup rowGroup);
    abstract public void addKeyValueMetaData(KeyValue kv);
    abstract public void setCreatedBy(String createdBy);
  }

  /**
   * Simple default consumer that sets the fields
   *
   */
  public static final class DefaultFileMetaDataConsumer extends FileMetaDataConsumer {
    private final FileMetaData md;

    public DefaultFileMetaDataConsumer(FileMetaData md) {
      this.md = md;
    }

    @Override
    public void setVersion(int version) {
      md.setVersion(version);
    }

    @Override
    public void setSchema(List<SchemaElement> schema) {
      md.setSchema(schema);
    }

    @Override
    public void setNumRows(long numRows) {
      md.setNum_rows(numRows);
    }

    @Override
    public void setCreatedBy(String createdBy) {
      md.setCreated_by(createdBy);
    }

    @Override
    public void addRowGroup(RowGroup rowGroup) {
      md.addToRow_groups(rowGroup);
    }

    @Override
    public void addKeyValueMetaData(KeyValue kv) {
      md.addToKey_value_metadata(kv);
    }
  }

  public static void readFileMetaData(InputStream from, FileMetaDataConsumer consumer) throws IOException {
    readFileMetaData(from, consumer, false);
  }

  public static void readFileMetaData(InputStream from, final FileMetaDataConsumer consumer, boolean skipRowGroups) throws IOException {
    try {
      DelegatingFieldConsumer eventConsumer = fieldConsumer()
      .onField(VERSION, new I32Consumer() {
        @Override
        public void consume(int value) {
          consumer.setVersion(value);
        }
      }).onField(SCHEMA, listOf(SchemaElement.class, new Consumer<List<SchemaElement>>() {
        @Override
        public void consume(List<SchemaElement> schema) {
          consumer.setSchema(schema);
        }
      })).onField(NUM_ROWS, new I64Consumer() {
        @Override
        public void consume(long value) {
          consumer.setNumRows(value);
        }
      }).onField(KEY_VALUE_METADATA, listElementsOf(struct(KeyValue.class, new Consumer<KeyValue>() {
        @Override
        public void consume(KeyValue kv) {
          consumer.addKeyValueMetaData(kv);
        }
      }))).onField(CREATED_BY, new StringConsumer() {
        @Override
        public void consume(String value) {
          consumer.setCreatedBy(value);
        }
      });
      if (!skipRowGroups) {
        eventConsumer = eventConsumer.onField(ROW_GROUPS, listElementsOf(struct(RowGroup.class, new Consumer<RowGroup>() {
          @Override
          public void consume(RowGroup rowGroup) {
            consumer.addRowGroup(rowGroup);
          }
        })));
      }
      new EventBasedThriftReader(protocol(from)).readStruct(eventConsumer);

    } catch (TException e) {
      throw new IOException("can not read FileMetaData: " + e.getMessage(), e);
    }
  }

  private static TProtocol protocol(OutputStream to) {
    return protocol(new TIOStreamTransport(to));
  }

  private static TProtocol protocol(InputStream from) {
    return protocol(new TIOStreamTransport(from));
  }

  private static InterningProtocol protocol(TIOStreamTransport t) {
    return new InterningProtocol(new TCompactProtocol(t));
  }

  private static <T extends TBase<?,?>> T read(InputStream from, T tbase) throws IOException {
    try {
      tbase.read(protocol(from));
      return tbase;
    } catch (TException e) {
      throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
    }
  }

  private static void write(TBase<?, ?> tbase, OutputStream to) throws IOException {
    try {
      tbase.write(protocol(to));
    } catch (TException e) {
      throw new IOException("can not write " + tbase, e);
    }
  }
}
