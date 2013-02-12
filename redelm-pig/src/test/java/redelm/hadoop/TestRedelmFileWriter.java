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
package redelm.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.parser.MessageTypeParser;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestRedelmFileWriter {
  private static final Log LOG = Log.getLog(TestRedelmFileWriter.class);

  public static class ValidatingPageConsumer extends PageConsumer {

    List<String> expected = new ArrayList<String>();
    int counter = 0;


    @Override
    public void consumePage(String[] path, int valueCount, BytesInput bytes) {
      assertEquals("at index "+counter, expected.get(counter), toString(path, valueCount, bytes));
      ++ counter;
    }

    private String toString(String[] path, int valueCount, BytesInput bytes) {
      try {
        return Arrays.toString(path) + " "+valueCount+" "+Arrays.toString(bytes.toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void add(String[] path, int valueCount, BytesInput bytes) {
      expected.add(toString(path, valueCount, bytes));
    }

    public void validate() {
      assertEquals(expected.size(), counter);
      expected.clear();
      counter = 0;
    }

  }

  @Test
  public void test() throws Exception {

    File testFile = new File("target/testRedelmFile").getAbsoluteFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required binary b;} required group c { required int64 d; }}");
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = { 0, 1, 2, 3};
    byte[] bytes2 = { 1, 2, 3, 4};
    byte[] bytes3 = { 2, 3, 4, 5};
    byte[] bytes4 = { 3, 4, 5, 6};
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    RedelmFileWriter w = new RedelmFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(c1, 5, codec);
    w.writeDataPage(2, 4, BytesInput.from(bytes1));
    w.writeDataPage(3, 4, BytesInput.from(bytes1));
    w.endColumn();
    w.startColumn(c2, 6, codec);
    w.writeDataPage(2, 4, BytesInput.from(bytes2));
    w.writeDataPage(3, 4, BytesInput.from(bytes2));
    w.writeDataPage(1, 4, BytesInput.from(bytes2));
    w.endColumn();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(c1, 7, codec);
    w.writeDataPage(7, 4, BytesInput.from(bytes3));
    w.endColumn();
    w.startColumn(c2, 8, codec);
    w.writeDataPage(8, 4, BytesInput.from(bytes4));
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());

    RedelmMetaData readFooter = RedelmFileReader.readFooter(configuration, path);
    assertEquals("footer: "+readFooter, 2, readFooter.getBlocks().size());

    ValidatingPageConsumer pageConsumer = new ValidatingPageConsumer();
    { // read first block of col #1
      pageConsumer.add(path1, 2, BytesInput.from(bytes1));
      pageConsumer.add(path1, 3, BytesInput.from(bytes1));

      RedelmFileReader r = new RedelmFileReader(configuration, path, Arrays.asList(readFooter.getBlocks().get(0)), Arrays.<String[]>asList(path1));
      assertEquals(3, r.readColumns(pageConsumer));
      pageConsumer.validate();
      assertEquals(0, r.readColumns(pageConsumer));
    }

    { // read all blocks of col #1 and #2

      pageConsumer.add(path1, 2, BytesInput.from(bytes1));
      pageConsumer.add(path1, 3, BytesInput.from(bytes1));
      pageConsumer.add(path2, 2, BytesInput.from(bytes2));
      pageConsumer.add(path2, 3, BytesInput.from(bytes2));
      pageConsumer.add(path2, 1, BytesInput.from(bytes2));

      RedelmFileReader r = new RedelmFileReader(configuration, path, readFooter.getBlocks(), Arrays.<String[]>asList(path1, path2));

      assertEquals(3, r.readColumns(pageConsumer));
      pageConsumer.validate();

      pageConsumer.add(path1, 7, BytesInput.from(bytes3));
      pageConsumer.add(path2, 8, BytesInput.from(bytes4));

      assertEquals(4, r.readColumns(pageConsumer));
      pageConsumer.validate();

      assertEquals(0, r.readColumns(pageConsumer));
    }
    PrintFooter.main(new String[] {path.toString()});
  }
}
