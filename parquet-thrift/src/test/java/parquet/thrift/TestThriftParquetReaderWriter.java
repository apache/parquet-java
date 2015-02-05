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
package parquet.thrift;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.hadoop.metadata.CompressionCodecName;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;

public class TestThriftParquetReaderWriter {

  @Test
  public void testWriteRead() throws IOException {
    Configuration configuration = new Configuration();
    Path f = new Path("target/test/TestThriftParquetReaderWriter");
    FileSystem fs = f.getFileSystem(configuration);
    if (fs.exists(f)) {
      fs.delete(f, true);
    }

    AddressBook original = new AddressBook(
        Arrays.asList(new Person(new Name("Bob", "Roberts"), 1, "bob@roberts.com", Arrays.asList(new PhoneNumber("5555555555"))))
        );

    { // write
      ThriftParquetWriter<AddressBook> thriftParquetWriter = new ThriftParquetWriter<AddressBook>(f, AddressBook.class, CompressionCodecName.UNCOMPRESSED);
      thriftParquetWriter.write(original);
      thriftParquetWriter.close();
    }

    { // read
      ThriftParquetReader<AddressBook> thriftParquetReader = new ThriftParquetReader<AddressBook>(f, AddressBook.class);
      AddressBook read = thriftParquetReader.read();
      Assert.assertEquals(original, read);
      thriftParquetReader.close();
    }

    { // read without providing a thrift class
      ThriftParquetReader<AddressBook> thriftParquetReader = new ThriftParquetReader<AddressBook>(f);
      AddressBook read = thriftParquetReader.read();
      Assert.assertEquals(original, read);
      thriftParquetReader.close();
    }
  }
}
