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
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

import redelm.column.ColumnDescriptor;
import redelm.parser.MessageTypeParser;
import redelm.schema.MessageType;

public class TestRedelmFileWriter {

  private static final String CODEC = GzipCodec.class.getName();

  @Test
  public void test() throws Exception {

    Path path = new Path(new File("target/testRedelmFile").getAbsoluteFile().toURI());
    Configuration configuration = new Configuration();
    FileSystem fileSystem = path.getFileSystem(configuration);

    FSDataOutputStream fout = fileSystem.create(path, true);

    MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required string b;} required group c { required int64 d; }}");
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = { 0, 1, 2, 3};
    byte[] bytes2 = { 1, 2, 3, 4};
    byte[] bytes3 = { 2, 3, 4, 5};
    byte[] bytes4 = { 3, 4, 5, 6};

    RedelmFileWriter w = new RedelmFileWriter(schema, fout, (CompressionCodec)Class.forName(CODEC).newInstance());
    w.start();
    w.startBlock(1);
    w.startColumn(c1, 1);
    w.startRepetitionLevels();
    w.write(bytes1, 0, bytes1.length);
    w.startDefinitionLevels();
    w.write(bytes1, 0, bytes1.length);
    w.startData();
    w.write(bytes1, 0, bytes1.length);
    w.endColumn();
    w.startColumn(c2, 1);
    w.startRepetitionLevels();
    w.write(bytes2, 0, bytes2.length);
    w.startDefinitionLevels();
    w.write(bytes2, 0, bytes2.length);
    w.startData();
    w.write(bytes2, 0, bytes2.length);
    w.endColumn();
    w.endBlock();
    w.startBlock(1);
    w.startColumn(c1, 1);
    w.startRepetitionLevels();
    w.write(bytes3, 0, bytes3.length);
    w.startDefinitionLevels();
    w.write(bytes3, 0, bytes3.length);
    w.startData();
    w.write(bytes3, 0, bytes3.length);
    w.endColumn();
    w.startColumn(c2, 1);
    w.startRepetitionLevels();
    w.write(bytes4, 0, bytes4.length);
    w.startDefinitionLevels();
    w.write(bytes4, 0, bytes4.length);
    w.startData();
    w.write(bytes4, 0, bytes4.length);
    w.endColumn();
    w.endBlock();
    w.end(new ArrayList<MetaDataBlock>());

    FSDataInputStream fin = fileSystem.open(path);

    RedelmMetaData readFooter = RedelmMetaData.fromMetaDataBlocks(RedelmFileReader.readFooter(fin, fileSystem.getFileStatus(path).getLen()));

    {
      assertEquals(2, readFooter.getBlocks().size());
      RedelmFileReader r = new RedelmFileReader(configuration, fin, Arrays.asList(readFooter.getBlocks().get(0)), Arrays.<String[]>asList(path1), CODEC);
      BlockData blockData = r.readColumns();
      List<ColumnData> cols = blockData.getColumns();
      System.out.println(cols);
      assertEquals(1, cols.size());
      assertEquals(bytes1.length, cols.get(0).getData().length);
      assertEquals(bytes1[0], cols.get(0).getData()[0]);
      assertNull(r.readColumns());
    }

    {
      RedelmFileReader r = new RedelmFileReader(configuration, fin, readFooter.getBlocks(), Arrays.<String[]>asList(path1, path2), CODEC);
      BlockData blockData1 = r.readColumns();
      List<ColumnData> cols1 = blockData1.getColumns();
      assertEquals(2, cols1.size());
      ColumnData c11 = cols1.get(0);
      assertEquals(bytes1[0], c11.getData()[0]);
      assertEquals(bytes1.length, c11.getData().length);
      ColumnData c12 = cols1.get(1);
      assertEquals(bytes2.length, c12.getData().length);
      assertEquals(bytes2[0], c12.getData()[0]);
      BlockData blockData2 = r.readColumns();
      List<ColumnData> cols2 = blockData2.getColumns();
      assertEquals(2, cols2.size());
      ColumnData c21 = cols2.get(0);
      assertEquals(bytes3.length, c21.getData().length);
      assertEquals(bytes3[0], c21.getData()[0]);
      ColumnData c22 = cols2.get(1);
      assertEquals(bytes4.length, c22.getData().length);
      assertEquals(bytes4[0], c22.getData()[0]);
      assertNull(r.readColumns());
    }
    PrintFooter.main(new String[] {path.toString()});
  }
}
