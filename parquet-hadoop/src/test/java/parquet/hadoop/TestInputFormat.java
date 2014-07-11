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
package parquet.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

import parquet.column.ColumnReader;
import parquet.column.Encoding;
import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.IntStatistics;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.FilterPredicate;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.ColumnPath;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.FileMetaData;
import parquet.io.ParquetDecodingException;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;

public class TestInputFormat {

  List<BlockMetaData> blocks;
  BlockLocation[] hdfsBlocks;
  FileStatus fileStatus;
  MessageType schema;
  FileMetaData fileMetaData;

  /*
    The test File contains 2-3 hdfs blocks based on the setting of each test, when hdfsBlock size is set to 50: [0-49][50-99]
    each row group is of size 10, so the rowGroups layout on hdfs is like:
    xxxxx xxxxx
    each x is a row group, each groups of x's is a hdfsBlock
   */
  @Before
  public void setUp() {
    blocks = new ArrayList<BlockMetaData>();
    for (int i = 0; i < 10; i++) {
      blocks.add(newBlock(i * 10, 10));
    }
    fileStatus = new FileStatus(100, false, 2, 50, 0, new Path("hdfs://foo.namenode:1234/bar"));
    schema = MessageTypeParser.parseMessageType("message doc { required binary foo; }");
    fileMetaData = new FileMetaData(schema, new HashMap<String, String>(), "parquet-mr");
  }

  @Test
  public void testThrowExceptionWhenMaxSplitSizeIsSmallerThanMinSplitSize() throws IOException {
    try {
      generateSplitByMinMaxSize(50, 49);
      fail("should throw exception when max split size is smaller than the min split size");
    } catch (ParquetDecodingException e) {
      assertEquals("maxSplitSize and minSplitSize should be positive and max should be greater or equal to the minSplitSize: maxSplitSize = 49; minSplitSize is 50"
              , e.getMessage());
    }
  }

  @Test
  public void testThrowExceptionWhenMaxSplitSizeIsNegative() throws IOException {
    try {
      generateSplitByMinMaxSize(-100, -50);
      fail("should throw exception when max split size is negative");
    } catch (ParquetDecodingException e) {
      assertEquals("maxSplitSize and minSplitSize should be positive and max should be greater or equal to the minSplitSize: maxSplitSize = -50; minSplitSize is -100"
              , e.getMessage());
    }
  }

  @Test
  public void testFilterPredicateConfiguration() throws IOException {
    Column<Integer> intColumn = intColumn("foo");
    FilterPredicate p = or(eq(intColumn, 7), eq(intColumn, 12));
    Configuration conf = new Configuration();
    ParquetInputFormat.setFilterPredicate(conf, p);
    FilterPredicate read = ParquetInputFormat.getFilterPredicate(conf);
    assertEquals(p, read);
  }

  /*
    aaaaa bbbbb
   */
  @Test
  public void testGenerateSplitsAlignedWithHDFSBlock() throws IOException {
    withHDFSBlockSize(50, 50);
    List<ParquetInputSplit> splits = generateSplitByMinMaxSize(50, 50);
    shouldSplitBlockSizeBe(splits, 5, 5);
    shouldSplitLocationBe(splits, 0, 1);
    shouldSplitLengthBe(splits, 50, 50);

    splits = generateSplitByMinMaxSize(0, Long.MAX_VALUE);
    shouldSplitBlockSizeBe(splits, 5, 5);
    shouldSplitLocationBe(splits, 0, 1);
    shouldSplitLengthBe(splits, 50, 50);

  }

  @Test
  public void testRowGroupNotAlignToHDFSBlock() throws IOException {
    //Test HDFS blocks size(51) is not multiple of row group size(10)
    withHDFSBlockSize(51, 51);
    List<ParquetInputSplit> splits = generateSplitByMinMaxSize(50, 50);
    shouldSplitBlockSizeBe(splits, 5, 5);
    shouldSplitLocationBe(splits, 0, 0);//for the second split, the first byte will still be in the first hdfs block, therefore the locations are both 0
    shouldSplitLengthBe(splits, 50, 50);

    //Test a rowgroup is greater than the hdfsBlock boundary
    withHDFSBlockSize(49, 49);
    splits = generateSplitByMinMaxSize(50, 50);
    shouldSplitBlockSizeBe(splits, 5, 5);
    shouldSplitLocationBe(splits, 0, 1);
    shouldSplitLengthBe(splits, 50, 50);

    /*
    aaaa bbbbb c
    for the 5th row group, the midpoint is 45, but the end of first hdfsBlock is 44, therefore a new split(b) will be created
    for 9th group, the mid point is 85, the end of second block is 88, so it's considered mainly in the 2nd hdfs block, and therefore inserted as
    a row group of split b
     */
    withHDFSBlockSize(44,44,44);
    splits = generateSplitByMinMaxSize(40, 50);
    shouldSplitBlockSizeBe(splits, 4, 5, 1);
    shouldSplitLocationBe(splits, 0, 0, 2);
    shouldSplitLengthBe(splits, 40, 50, 10);
  }

  /*
    when min size is 55, max size is 56, the first split will be generated with 6 row groups(size of 10 each), which satisfies split.size>min.size, but not split.size<max.size
    aaaaa abbbb
   */
  @Test
  public void testGenerateSplitsNotAlignedWithHDFSBlock() throws IOException, InterruptedException {
    withHDFSBlockSize(50, 50);
    List<ParquetInputSplit> splits = generateSplitByMinMaxSize(55, 56);
    shouldSplitBlockSizeBe(splits, 6, 4);
    shouldSplitLocationBe(splits, 0, 1);
    shouldSplitLengthBe(splits, 60, 40);

    withHDFSBlockSize(51, 51);
    splits = generateSplitByMinMaxSize(55, 56);
    shouldSplitBlockSizeBe(splits, 6, 4);
    shouldSplitLocationBe(splits, 0, 1);//since a whole row group of split a is added to the second hdfs block, so the location of split b is still 1
    shouldSplitLengthBe(splits, 60, 40);

    withHDFSBlockSize(49, 49, 49);
    splits = generateSplitByMinMaxSize(55, 56);
    shouldSplitBlockSizeBe(splits, 6, 4);
    shouldSplitLocationBe(splits, 0, 1);
    shouldSplitLengthBe(splits, 60, 40);

  }

  /*
    when the max size is set to be 30, first split will be of size 30,
    and when creating second split, it will try to align it to second hdfsBlock, and therefore generates a split of size 20
    aaabb cccdd
   */
  @Test
  public void testGenerateSplitsSmallerThanMaxSizeAndAlignToHDFS() throws Exception {
    withHDFSBlockSize(50, 50);
    List<ParquetInputSplit> splits = generateSplitByMinMaxSize(18, 30);
    shouldSplitBlockSizeBe(splits, 3, 2, 3, 2);
    shouldSplitLocationBe(splits, 0, 0, 1, 1);
    shouldSplitLengthBe(splits, 30, 20, 30, 20);

    /*
    aaabb cccdd
         */
    withHDFSBlockSize(51, 51);
    splits = generateSplitByMinMaxSize(18, 30);
    shouldSplitBlockSizeBe(splits, 3, 2, 3, 2);
    shouldSplitLocationBe(splits, 0, 0, 0, 1);//the first byte of split c is in the first hdfs block
    shouldSplitLengthBe(splits, 30, 20, 30, 20);

    /*
    aaabb cccdd
     */
    withHDFSBlockSize(49, 49, 49);
    splits = generateSplitByMinMaxSize(18, 30);
    shouldSplitBlockSizeBe(splits, 3, 2, 3, 2);
    shouldSplitLocationBe(splits, 0, 0, 1, 1);
    shouldSplitLengthBe(splits, 30, 20, 30, 20);
  }

  /*
    when the min size is set to be 25, so the second split can not be aligned with the boundary of hdfs block, there for split of size 30 will be created as the 3rd split.
    aaabb bcccd
   */
  @Test
  public void testGenerateSplitsCrossHDFSBlockBoundaryToSatisfyMinSize() throws Exception {
    withHDFSBlockSize(50, 50);
    List<ParquetInputSplit> splits = generateSplitByMinMaxSize(25, 30);
    shouldSplitBlockSizeBe(splits, 3, 3, 3, 1);
    shouldSplitLocationBe(splits, 0, 0, 1, 1);
    shouldSplitLengthBe(splits, 30, 30, 30, 10);
  }

  /*
    when rowGroups size is 10, but min split size is 10, max split size is 18, it will create splits of size 20 and of size 10 and align with hdfsBlocks
    aabbc ddeef
   */
  @Test
  public void testMultipleRowGroupsInABlockToAlignHDFSBlock() throws Exception {
    withHDFSBlockSize(50, 50);
    List<ParquetInputSplit> splits = generateSplitByMinMaxSize(10, 18);
    shouldSplitBlockSizeBe(splits, 2, 2, 1, 2, 2, 1);
    shouldSplitLocationBe(splits, 0, 0, 0, 1, 1, 1);
    shouldSplitLengthBe(splits, 20, 20, 10, 20, 20, 10);

    /*
    aabbc ddeef
    notice the first byte of split d is in the first hdfs block:
    when adding the 6th row group, although the first byte of it is in the first hdfs block
    , but the mid point of the row group is in the second hdfs block, there for a new split(d) is created including that row group
     */
    withHDFSBlockSize(51, 51);
    splits = generateSplitByMinMaxSize(10, 18);
    shouldSplitBlockSizeBe(splits, 2, 2, 1, 2, 2, 1);
    shouldSplitLocationBe(splits, 0, 0, 0, 0, 1, 1);// location of split d should be 0, since the first byte is in the first hdfs block
    shouldSplitLengthBe(splits, 20, 20, 10, 20, 20, 10);

    /*
    aabbc ddeef
    same as the case where block sizes are 50 50
     */
    withHDFSBlockSize(49, 49);
    splits = generateSplitByMinMaxSize(10, 18);
    shouldSplitBlockSizeBe(splits, 2, 2, 1, 2, 2, 1);
    shouldSplitLocationBe(splits, 0, 0, 0, 1, 1, 1);
    shouldSplitLengthBe(splits, 20, 20, 10, 20, 20, 10);
  }

  @Test
  public void testLoadFilterPredicate() {
    Column<Integer> foo = intColumn("foo");
    FilterPredicate p = or(eq(foo, 10), eq(foo, 11));

    Configuration conf = new Configuration();
    ParquetInputFormat.setFilterPredicate(conf, p);
    FilterPredicate loaded = ParquetInputFormat.loadFilterPredicate(conf, "");
    assertEquals(p, loaded);

    conf = new Configuration();
    ParquetInputFormat.setFilterPredicate(conf, not(p));
    loaded = ParquetInputFormat.loadFilterPredicate(conf, "");
    assertEquals(and(notEq(foo, 10), notEq(foo, 11)), loaded);

    assertNull(ParquetInputFormat.loadFilterPredicate(new Configuration(), ""));
  }

  public static final class DummyUnboundRecordFilter implements UnboundRecordFilter {
    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
      return null;
    }
  }

  @Test
  public void testOnlyOneKindOfFilterSupported() throws Exception {
    Column<Integer> foo = intColumn("foo");
    FilterPredicate p = or(eq(foo, 10), eq(foo, 11));

    Job job = new Job();

    Configuration conf = job.getConfiguration();
    ParquetInputFormat.setUnboundRecordFilter(job, DummyUnboundRecordFilter.class);
    try {
      ParquetInputFormat.setFilterPredicate(conf, p);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("You cannot provide a FilterPredicate after providing an UnboundRecordFilter", e.getMessage());
    }

    job = new Job();
    conf = job.getConfiguration();

    ParquetInputFormat.setFilterPredicate(conf, p);
    try {
      ParquetInputFormat.setUnboundRecordFilter(job, DummyUnboundRecordFilter.class);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("You cannot provide an UnboundRecordFilter after providing a FilterPredicate", e.getMessage());
    }

  }

  private BlockMetaData makeBlockFromStats(IntStatistics stats, long valueCount) {
    BlockMetaData blockMetaData = new BlockMetaData();

    ColumnChunkMetaData column = ColumnChunkMetaData.get(ColumnPath.get("foo"),
        PrimitiveTypeName.INT32,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(Arrays.asList(Encoding.PLAIN)),
        stats,
        100l, 100l, valueCount, 100l, 100l);
    blockMetaData.addColumn(column);
    blockMetaData.setTotalByteSize(200l);
    blockMetaData.setRowCount(valueCount);
    return blockMetaData;
  }

  @Test
  public void testApplyRowGroupFilters() {

    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

    IntStatistics stats1 = new IntStatistics();
    stats1.setMinMax(10, 100);
    stats1.setNumNulls(4);
    BlockMetaData b1 = makeBlockFromStats(stats1, 301);
    blocks.add(b1);

    IntStatistics stats2 = new IntStatistics();
    stats2.setMinMax(8, 102);
    stats2.setNumNulls(0);
    BlockMetaData b2 = makeBlockFromStats(stats2, 302);
    blocks.add(b2);

    IntStatistics stats3 = new IntStatistics();
    stats3.setMinMax(100, 102);
    stats3.setNumNulls(12);
    BlockMetaData b3 = makeBlockFromStats(stats3, 303);
    blocks.add(b3);


    IntStatistics stats4 = new IntStatistics();
    stats4.setMinMax(0, 0);
    stats4.setNumNulls(304);
    BlockMetaData b4 = makeBlockFromStats(stats4, 304);
    blocks.add(b4);


    IntStatistics stats5 = new IntStatistics();
    stats5.setMinMax(50, 50);
    stats5.setNumNulls(7);
    BlockMetaData b5 = makeBlockFromStats(stats5, 305);
    blocks.add(b5);

    IntStatistics stats6 = new IntStatistics();
    stats6.setMinMax(0, 0);
    stats6.setNumNulls(12);
    BlockMetaData b6 = makeBlockFromStats(stats6, 306);
    blocks.add(b6);

    MessageType schema = MessageTypeParser.parseMessageType("message Document { optional int32 foo; }");
    Column<Integer> foo = intColumn("foo");

    List<BlockMetaData> filtered = ParquetInputFormat.applyRowGroupFilters(eq(foo, 50), schema, blocks);
    assertEquals(Arrays.asList(b1, b2, b5), filtered);

    filtered = ParquetInputFormat.applyRowGroupFilters(notEq(foo, 50), schema, blocks);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = ParquetInputFormat.applyRowGroupFilters(eq(foo, null), schema, blocks);
    assertEquals(Arrays.asList(b1, b3, b4, b5, b6), filtered);

    filtered = ParquetInputFormat.applyRowGroupFilters(notEq(foo, null), schema, blocks);
    assertEquals(Arrays.asList(b1, b2, b3, b5, b6), filtered);

    filtered = ParquetInputFormat.applyRowGroupFilters(eq(foo, 0), schema, blocks);
    assertEquals(Arrays.asList(b6), filtered);

  }

  private List<ParquetInputSplit> generateSplitByMinMaxSize(long min, long max) throws IOException {
    return ParquetInputFormat.generateSplits(
            blocks, hdfsBlocks, fileStatus, fileMetaData, schema.toString(), new HashMap<String, String>() {{
              put("specific", "foo");
            }}, min, max
    );
  }

  private void shouldSplitBlockSizeBe(List<ParquetInputSplit> splits, int... sizes) {
    assertEquals(sizes.length, splits.size());
    for (int i = 0; i < sizes.length; i++) {
      assertEquals(sizes[i], splits.get(i).getBlocks().size());
      assertEquals("foo", splits.get(i).getReadSupportMetadata().get("specific"));
    }
  }

  private void shouldSplitLocationBe(List<ParquetInputSplit> splits, int... locations) throws IOException {
    assertEquals(locations.length, splits.size());
    for (int i = 0; i < locations.length; i++) {
      assertEquals("[foo" + locations[i] + ".datanode, bar" + locations[i] + ".datanode]", Arrays.toString(splits.get(i).getLocations()));
    }
  }

  private void shouldSplitLengthBe(List<ParquetInputSplit> splits, int... lengths) {
    assertEquals(lengths.length, splits.size());
    for (int i = 0; i < lengths.length; i++) {
      assertEquals(lengths[i], splits.get(i).getLength());
    }
  }

  private void withHDFSBlockSize(long... blockSizes) {
    hdfsBlocks = new BlockLocation[blockSizes.length];
    long offset = 0;
    for (int i = 0; i < blockSizes.length; i++) {
      long blockSize = blockSizes[i];
      hdfsBlocks[i] = new BlockLocation(new String[0], new String[]{"foo" + i + ".datanode", "bar" + i + ".datanode"}, offset, blockSize);
      offset += blockSize;
    }
  }

  private BlockMetaData newBlock(long start, long compressedBlockSize) {
    BlockMetaData blockMetaData = new BlockMetaData();
    long uncompressedSize = compressedBlockSize * 2;//assuming the compression ratio is 2
    ColumnChunkMetaData column = ColumnChunkMetaData.get(ColumnPath.get("foo"),
                                                         PrimitiveTypeName.BINARY,
                                                         CompressionCodecName.GZIP,
                                                         new HashSet<Encoding>(Arrays.asList(Encoding.PLAIN)),
                                                         new BinaryStatistics(),
                                                         start, 0l, 0l, compressedBlockSize, uncompressedSize);
    blockMetaData.addColumn(column);
    blockMetaData.setTotalByteSize(uncompressedSize);
    return blockMetaData;
  }
}
