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
package parquet.thrift;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;
import org.junit.Assert;
import org.junit.Test;

import parquet.hadoop.ParquetReader;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.ThriftReadSupport;
import parquet.thrift.test.EmptyStruct;
import parquet.thrift.test.UnionWithEmptyStruct;

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
      ParquetReader<AddressBook> thriftParquetReader = ThriftParquetReader.build(f, AddressBook.class).build();
      AddressBook read = thriftParquetReader.read();
      Assert.assertEquals(original, read);
      thriftParquetReader.close();
    }

    { // read without providing a thrift class
      ParquetReader<TBase<?, ?>> thriftParquetReader = ThriftParquetReader.build(f).build();
      TBase<?, ?> read = thriftParquetReader.read();
      Assert.assertEquals(original, read);
      thriftParquetReader.close();
    }
  }

  @Test
  public void testWriteReadEmptyStruct() throws IOException {
    Configuration configuration = new Configuration();
    Path f = new Path("target/test/TestThriftParquetReaderWriterEmptyStruct");
    FileSystem fs = f.getFileSystem(configuration);
    if (fs.exists(f)) {
      fs.delete(f, true);
    }

    UnionWithEmptyStruct empty = new UnionWithEmptyStruct();
    empty.setEmpty(new EmptyStruct());
    UnionWithEmptyStruct name = new UnionWithEmptyStruct();
    name.setName(new parquet.thrift.test.Name("Bob"));
    List<UnionWithEmptyStruct> originals = asList(empty, name);

    { // write
      ThriftParquetWriter<UnionWithEmptyStruct> thriftParquetWriter = new ThriftParquetWriter<UnionWithEmptyStruct>(f, UnionWithEmptyStruct.class, CompressionCodecName.UNCOMPRESSED);
      for (UnionWithEmptyStruct original : originals) {
        thriftParquetWriter.write(original);
      }
      thriftParquetWriter.close();
    }

    { // read
      ParquetReader<UnionWithEmptyStruct> thriftParquetReader = ThriftParquetReader.build(f, UnionWithEmptyStruct.class).build();
      for (int i = 0; i < originals.size(); i++) {
        UnionWithEmptyStruct original = originals.get(i);
        UnionWithEmptyStruct read = thriftParquetReader.read();
        Assert.assertEquals(original, read);
      }
      thriftParquetReader.close();
    }

  }
}
