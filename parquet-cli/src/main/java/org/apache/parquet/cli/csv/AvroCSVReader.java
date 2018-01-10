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

package org.apache.parquet.cli.csv;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.parquet.cli.util.RuntimeIOException;
import org.apache.avro.Schema;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class AvroCSVReader<E> implements Iterator<E>, Iterable<E>, Closeable {

  private final boolean reuseRecords;
  private final CSVReader reader;
  private final RecordBuilder<E> builder;
  private boolean hasNext = false;
  private String[] next = null;
  private E record = null;

  public AvroCSVReader(InputStream stream, CSVProperties props,
                       Schema schema, Class<E> type, boolean reuseRecords) {
    this.reader = AvroCSV.newReader(stream, props);
    this.reuseRecords = reuseRecords;

    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");

    List<String> header = null;
    if (props.useHeader) {
      this.hasNext = advance();
      header = Lists.newArrayList(next);
    } else if (props.header != null) {
      try {
        header = Lists.newArrayList(
            AvroCSV.newParser(props).parseLine(props.header));
      } catch (IOException e) {
        throw new RuntimeIOException(
            "Failed to parse header from properties: " + props.header, e);
      }
    }

    this.builder = new RecordBuilder<>(schema, type, header);

    // initialize by reading the first record
    this.hasNext = advance();
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public E next() {
    if (!hasNext) {
      throw new NoSuchElementException();
    }

    try {
      if (reuseRecords) {
        this.record = builder.makeRecord(next, record);
        return record;
      } else {
        return builder.makeRecord(next, null);
      }
    } finally {
      this.hasNext = advance();
    }
  }

  private boolean advance() {
    try {
      next = reader.readNext();
    } catch (IOException ex) {
      throw new RuntimeIOException("Could not read record", ex);
    }
    return (next != null);
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeIOException("Cannot close reader", e);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not implemented.");
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }
}
