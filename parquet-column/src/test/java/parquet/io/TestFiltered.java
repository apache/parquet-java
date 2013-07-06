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

import org.junit.Test;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.mem.MemPageStore;
import parquet.example.data.Group;
import parquet.example.data.GroupWriter;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.io.api.RecordMaterializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static parquet.example.Paper.r1;
import static parquet.example.Paper.r2;
import static parquet.example.Paper.schema;
import static parquet.filter.AndRecordFilter.and;
import static parquet.filter.PagedRecordFilter.page;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.filter.ColumnRecordFilter.column;

public class TestFiltered {

  @Test
  public void testFilterOnInteger() {
    MemPageStore memPageStore = new MemPageStore();
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    writeTestRecords(memPageStore, columnIO, 1);

    // Get first record
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            column("DocId", equalTo(10l)));

    Group actual1=  recordReader.read();
    assertNull( "There should be no more records as r2 filtered out", recordReader.read());
    assertEquals("filtering did not return the correct record", r1.toString(), actual1.toString());

    // Get second record
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            column("DocId", equalTo(20l)));

    Group actual2 = recordReader.read();
    assertNull( "There should be no more records as r1 filtered out", recordReader.read());
    assertEquals("filtering did not return the correct record", r2.toString(), actual2.toString());

  }

  @Test
  public void testFilterOnString() {
    MemPageStore memPageStore = new MemPageStore();
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    writeTestRecords(memPageStore, columnIO, 1);

    // First try matching against the A url in record 1
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            column("Name.Url", equalTo("http://A")));

    Group actual1 = recordReader.read();
    assertNull( "There should be no more records as r2 filtered out", recordReader.read());
    assertEquals("filtering did not return the correct record", r1.toString(), actual1.toString());

    // Second try matching against the B url in record 1 - it should fail as we only match
    // against the first instance of a
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            column("Name.Url", equalTo("http://B")));

    assertNull( "There should be no matching records", recordReader.read());

    // Finally try matching against the C url in record 2
    recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            column("Name.Url", equalTo("http://C")));

    Group actual2 =  recordReader.read();
    assertNull( "There should be no more records as r1 filtered out", recordReader.read());
    assertEquals("filtering did not return the correct record", r2.toString(), actual2.toString());
  }

  @Test
  public void testPaged() {
    MemPageStore memPageStore = new MemPageStore();
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    writeTestRecords(memPageStore, columnIO, 6);

    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
                                 page(4, 4));

    int count = 0;
    while ( count < 2 ) { // starts at position 4 which should be r2
      assertEquals("expecting record2", r2.toString(), recordReader.read().toString());
      assertEquals("expecting record1", r1.toString(), recordReader.read().toString());
      count++;
    }
    assertNull("There should be no more records", recordReader.read());
  }

  @Test
  public void testFilteredAndPaged() {
    MemPageStore memPageStore = new MemPageStore();
    MessageColumnIO columnIO =  new ColumnIOFactory(true).getColumnIO(schema);
    writeTestRecords(memPageStore, columnIO, 8);

    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);
    RecordReaderImplementation<Group> recordReader = (RecordReaderImplementation<Group>)
        columnIO.getRecordReader(memPageStore, recordConverter,
            and(column("DocId", equalTo(10l)), page(2, 4)));

    int count = 0;
    while ( count < 4 ) { // starts at position 4 which should be r2
      assertEquals("expecting 4 x record1", r1.toString(), recordReader.read().toString());
      count++;
    }
    assertNull( "There should be no more records", recordReader.read());
  }

  private void writeTestRecords(MemPageStore memPageStore, MessageColumnIO columnIO, int number) {
    ColumnWriteStoreImpl columns = new ColumnWriteStoreImpl(memPageStore, 800, 800, false);

    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
    for ( int i = 0; i < number; i++ ) {
      groupWriter.write(r1);
      groupWriter.write(r2);
    }
    columns.flush();
  }
}
