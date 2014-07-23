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
package parquet.io;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.mem.MemPageStore;
import parquet.example.data.Group;
import parquet.example.data.GroupWriter;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.filter.ColumnPredicates.LongPredicateFunction;
import parquet.filter.ColumnPredicates.PredicateFunction;
import parquet.filter2.compat.FilterCompat;
import parquet.io.api.RecordMaterializer;

import static org.junit.Assert.assertEquals;
import static parquet.example.Paper.r1;
import static parquet.example.Paper.r2;
import static parquet.example.Paper.schema;
import static parquet.filter.AndRecordFilter.and;
import static parquet.filter.ColumnPredicates.applyFunctionToLong;
import static parquet.filter.ColumnPredicates.applyFunctionToString;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.filter.ColumnRecordFilter.column;
import static parquet.filter.NotRecordFilter.not;
import static parquet.filter.OrRecordFilter.or;
import static parquet.filter.PagedRecordFilter.page;

public class TestFiltered {

  /* Class that implements applyFunction filter for long. Checks for long greater than 15. */
  public class LongGreaterThan15Predicate implements LongPredicateFunction {
    @Override
    public boolean functionToApply(long input) {
      return input > 15;
    }
  };

  /* Class that implements applyFunction filter for string. Checks for string ending in 'A'. */
  public class StringEndsWithAPredicate implements PredicateFunction<String> {
    @Override
    public boolean functionToApply(String input) {
      return input.endsWith("A");
    }
  };

  private List<Group> readAll(RecordReader<Group> reader) {
    List<Group> result = new ArrayList<Group>();
    Group g;
    while ((g = reader.read()) != null) {
      result.add(g);
    }
    return result;
  }

  private void readOne(RecordReader<Group> reader, String message, Group expected) {
    List<Group> result = readAll(reader);
    assertEquals(message + ": " + result, 1, result.size());
    assertEquals("filtering did not return the correct record", expected.toString(), result.get(0).toString());
  }

  @Test
  public void testFilterOnInteger() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 1);

    // Get first record
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter, FilterCompat.get(column("DocId", equalTo(10l))));

    readOne(recordReader, "r2 filtered out", r1);

    // Get second record
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("DocId", equalTo(20l))));

    readOne(recordReader, "r1 filtered out", r2);

  }

  @Test
  public void testApplyFunctionFilterOnLong() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 1);

    // Get first record
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("DocId", equalTo(10l))));

    readOne(recordReader, "r2 filtered out", r1);

    // Get second record
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("DocId", applyFunctionToLong(new LongGreaterThan15Predicate()))));

    readOne(recordReader, "r1 filtered out", r2);
  }

  @Test
  public void testFilterOnString() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 1);

    // First try matching against the A url in record 1
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("Name.Url", equalTo("http://A"))));

    readOne(recordReader, "r2 filtered out", r1);

    // Second try matching against the B url in record 1 - it should fail as we only match
    // against the first instance of a
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("Name.Url", equalTo("http://B"))));

    List<Group> all = readAll(recordReader);
    assertEquals("There should be no matching records: " + all , 0, all.size());

    // Finally try matching against the C url in record 2
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("Name.Url", equalTo("http://C"))));

    readOne(recordReader, "r1 filtered out", r2);

  }

  @Test
  public void testApplyFunctionFilterOnString() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 1);

    // First try matching against the A url in record 1
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("Name.Url", applyFunctionToString(new StringEndsWithAPredicate()))));

    readOne(recordReader, "r2 filtered out", r1);

    // Second try matching against the B url in record 1 - it should fail as we only match
    // against the first instance of a
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("Name.Url", equalTo("http://B"))));

    List<Group> all = readAll(recordReader);
    assertEquals("There should be no matching records: " + all , 0, all.size());

    // Finally try matching against the C url in record 2
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(column("Name.Url", equalTo("http://C"))));

    readOne(recordReader, "r1 filtered out", r2);

  }

  @Test
  public void testPaged() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 6);

    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(page(4, 4)));

    List<Group> all = readAll(recordReader);
    assertEquals("expecting records " + all, 4, all.size());
    for (int i = 0; i < all.size(); i++) {
      assertEquals("expecting record", (i%2 == 0 ? r2 : r1).toString(), all.get(i).toString());
    }
  }

  @Test
  public void testFilteredAndPaged() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 8);

    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(and(column("DocId", equalTo(10l)), page(2, 4))));

    List<Group> all = readAll(recordReader);
    assertEquals("expecting 4 records " + all, 4, all.size());
    for (int i = 0; i < all.size(); i++) {
      assertEquals("expecting record1", r1.toString(), all.get(i).toString());
    }

  }

  @Test
  public void testFilteredOrPaged() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 8);

    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(or(column("DocId", equalTo(10l)),
                column("DocId", equalTo(20l)))));

    List<Group> all = readAll(recordReader);
    assertEquals("expecting 8 records " + all, 16, all.size());
    for (int i = 0; i < all.size () / 2; i++) {
      assertEquals("expecting record1", r1.toString(), all.get(2 * i).toString());
      assertEquals("expecting record2", r2.toString(), all.get(2 * i + 1).toString());
    }
  }

  @Test
  public void testFilteredNotPaged() {
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    MemPageStore memPageStore = writeTestRecords(columnIO, 8);

    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            FilterCompat.get(not(column("DocId", equalTo(10l)))));

    List<Group> all = readAll(recordReader);
    assertEquals("expecting 8 records " + all, 8, all.size());
    for (int i = 0; i < all.size(); i++) {
      assertEquals("expecting record2", r2.toString(), all.get(i).toString());
    }
  }

  private MemPageStore writeTestRecords(MessageColumnIO columnIO, int number) {
    MemPageStore memPageStore = new MemPageStore(number * 2);
    ColumnWriteStoreImpl columns = new ColumnWriteStoreImpl(memPageStore, 800, 800, 800, false, WriterVersion.PARQUET_1_0);

    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
    for ( int i = 0; i < number; i++ ) {
      groupWriter.write(r1);
      groupWriter.write(r2);
    }
    columns.flush();
    return memPageStore;
  }
}
