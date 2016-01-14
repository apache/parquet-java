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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.ColumnPredicates;
import org.apache.parquet.filter.ColumnRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestInternalParquetRecordReader {

  private PrimitiveType createPrimitiveType(String name) {
    return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, name);
  }

  private class DummyReadSupport extends ReadSupport<Integer> {

    final MessageType requestedSchema;

    public DummyReadSupport(MessageType requestedSchema) {
      this.requestedSchema = requestedSchema;
    }

    @Override
    public RecordMaterializer<Integer> prepareForRead(
        Configuration configuration,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadSupport.ReadContext readContext) {
      return null;
    }

    public ReadSupport.ReadContext init(InitContext context) {
      return new ReadSupport.ReadContext(requestedSchema);
    }
  }

  private static class DummyColumnPredicate implements ColumnPredicates.Predicate {

    @Override
    public boolean apply(ColumnReader input) {
      return false;
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testThrowExceptionForParquet425FilterPredicateCompatRuntimeException() throws IOException {
    FilterCompat.Filter filter = FilterCompat.get(FilterApi.eq(FilterApi.intColumn("b"), 1));
    testThrowExceptionForParquet425RuntimeException(filter);
  }


  @Test
  public void testThrowExceptionForParquet425UnboundRecordFilterCompatRuntimeException() throws IOException {
    FilterCompat.Filter filter = FilterCompat.get(ColumnRecordFilter.column("b", new DummyColumnPredicate()));
    testThrowExceptionForParquet425RuntimeException(filter);
  }

  @Test
  public void testThrowExceptionForParquet425NoOpFilterNullPointerException() throws IOException {
    testThrowExceptionForParquet425NullPointerException(FilterCompat.NOOP);
  }

  private void testThrowExceptionForParquet425RuntimeException(FilterCompat.Filter filter) throws IOException {
    final MessageType fileSchema = new MessageType("root", createPrimitiveType("a"), createPrimitiveType("b"));
    final MessageType requestedSchema = new MessageType("root", createPrimitiveType("a"));

    ReadSupport<Integer> readSupport = new DummyReadSupport(requestedSchema);
    InternalParquetRecordReader<Integer> reader = new InternalParquetRecordReader<Integer>(readSupport, filter);
    FileMetaData fileMetaData = new FileMetaData(fileSchema, new HashMap<String, String>(), "createdBy");

    String msg = "Warning: filter predicate contains columns not specified in projection! "
                 + "This often indicates incorrect data filter due to Parquet-425; "
                 + "for now please work around this by adding all predicates columns into the projection.";
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(msg);
    reader.initialize(fileSchema, fileMetaData, null, null, null);
  }

  private void testThrowExceptionForParquet425NullPointerException(FilterCompat.Filter filter) throws IOException {
    final MessageType fileSchema = new MessageType("root", createPrimitiveType("a"), createPrimitiveType("b"));
    final MessageType requestedSchema = new MessageType("root", createPrimitiveType("a"));

    ReadSupport<Integer> readSupport = new DummyReadSupport(requestedSchema);
    InternalParquetRecordReader<Integer> reader = new InternalParquetRecordReader<Integer>(readSupport, filter);
    FileMetaData fileMetaData = new FileMetaData(fileSchema, new HashMap<String, String>(), "createdBy");

    thrown.expect(NullPointerException.class);
    reader.initialize(fileSchema, fileMetaData, null, null, null);
  }

  @Test
  public void testThrowExceptionForParquet425NormalFlow() throws IOException {
    final MessageType fileSchema = new MessageType("root", createPrimitiveType("a"), createPrimitiveType("b"));
    final MessageType requestedSchema = new MessageType("root", createPrimitiveType("a"), createPrimitiveType("b"));
    FilterCompat.Filter filter = FilterCompat.get(FilterApi.eq(FilterApi.intColumn("b"), 1));

    ReadSupport<Integer> readSupport = new DummyReadSupport(requestedSchema);
    InternalParquetRecordReader<Integer> reader = new InternalParquetRecordReader<Integer>(readSupport, filter);
    FileMetaData fileMetaData = new FileMetaData(fileSchema, new HashMap<String, String>(), "createdBy");

    // should pass throwing "Warning: filter predicate contains columns not specified in projection! "...
    // should throw NullPointerException because we are passing nulls into initialize(...)
    thrown.expect(NullPointerException.class);
    reader.initialize(fileSchema, fileMetaData, null, null, null);
  }
}