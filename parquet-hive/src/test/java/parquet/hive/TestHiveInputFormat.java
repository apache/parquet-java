/**
 * Copyright 2013 Criteo.
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
package parquet.hive;

import static org.junit.Assert.assertEquals;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.PrintFooter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 *
 * TestHiveInputFormat
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
// TODO need to test the ParquetHiveSerde and objectInspector. Need to understand how to write a file :)
public class TestHiveInputFormat {
    //    private static Configuration conf = new Configuration();
    // Data

    //    private static ParquetHiveSerDe serDe = new ParquetHiveSerDe();

    static final Log LOG = LogFactory.getLog(TestHiveInputFormat.class);


    @Test
    public void testWriteRead() throws Exception {

        final File testFile = new File("target/test/TestParquetFileWriter/testParquetFile").getAbsoluteFile();
        testFile.delete();

        final Path path = new Path(testFile.toURI());
        final Configuration configuration = new Configuration();

        final MessageType schema = MessageTypeParser.parseMessageType("message m { optional int32 a ; optional double b; optional binary c; optional boolean d;}");

        final List<ColumnDescriptor> rootColumn = schema. getColumns();

        LOG.error(rootColumn);
        final byte[] bytes1 = { 0, 1, 2, 3};
        final byte[] bytes2 = { 1, 2, 3, 4, 1, 2, 3, 4};
        final byte[] bytes3 = new String("parquet_test").getBytes("UTF-8");
        final byte[] bytes4 = { 1 };
        final CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        final ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
        w.start();
        w.startBlock(11);
        w.startColumn(rootColumn.get(0), 5, codec);
        w.writeDataPage(2, 4, BytesInput.from(bytes1), BIT_PACKED, BIT_PACKED, PLAIN);
        w.writeDataPage(3, 4, BytesInput.from(bytes1), BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.startColumn(rootColumn.get(1), 6, codec);
        w.writeDataPage(2, bytes2.length, BytesInput.from(bytes2), BIT_PACKED, BIT_PACKED, PLAIN);
        w.writeDataPage(3, bytes2.length, BytesInput.from(bytes2), BIT_PACKED, BIT_PACKED, PLAIN);
        w.writeDataPage(1, bytes2.length, BytesInput.from(bytes2), BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.endBlock();
        w.startBlock(15);
        //        w.startColumn(c1, 7, codec);
        w.writeDataPage(7, bytes3.length, BytesInput.from(bytes3), BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        //        w.startColumn(c2, 8, codec);
        w.writeDataPage(8, 1, BytesInput.from(bytes4), BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.endBlock();
        w.end(new HashMap<String, String>());

        final ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
        assertEquals("footer: "+readFooter, 2, readFooter.getBlocks().size());

        { // read first block of col #1
            //            final ParquetFileReader r = new ParquetFileReader(configuration, path, Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(schema.getColumnDescription(path1)));
            //            final PageReadStore pages = r.readNextRowGroup();
            //            assertEquals(3, pages.getRowCount());
            //            validateContains(schema, pages, path1, 2, BytesInput.from(bytes1));
            //            validateContains(schema, pages, path1, 3, BytesInput.from(bytes1));
            //            assertNull(r.readNextRowGroup());
        }

        { // read all blocks of col #1 and #2

            //            final ParquetFileReader r = new ParquetFileReader(configuration, path, readFooter.getBlocks(), Arrays.asList(schema.getColumnDescription(path1), schema.getColumnDescription(path2)));

            //            PageReadStore pages = r.readNextRowGroup();
            //            assertEquals(3, pages.getRowCount());
            //            validateContains(schema, pages, path1, 2, BytesInput.from(bytes1));
            //            validateContains(schema, pages, path1, 3, BytesInput.from(bytes1));
            //            validateContains(schema, pages, path2, 2, BytesInput.from(bytes2));
            //            validateContains(schema, pages, path2, 3, BytesInput.from(bytes2));
            //            validateContains(schema, pages, path2, 1, BytesInput.from(bytes2));
            //
            //            pages = r.readNextRowGroup();
            //            assertEquals(4, pages.getRowCount());
            //
            //            validateContains(schema, pages, path1, 7, BytesInput.from(bytes3));
            ////            validateContains(schema, pages, path2, 8, BytesInput.from(bytes4));

            //            assertNull(r.readNextRowGroup());
        }
        PrintFooter.main(new String[] {path.toString()});
    }
    //    private void validateContains(final MessageType schema, final PageReadStore pages, final String[] path, final int values, final BytesInput bytes) throws IOException {
    //        final PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    //        final Page page = pageReader.readPage();
    //        assertEquals(values, page.getValueCount());
    //        assertArrayEquals(bytes.toByteArray(), page.getBytes().toByteArray());
    //    }

}
