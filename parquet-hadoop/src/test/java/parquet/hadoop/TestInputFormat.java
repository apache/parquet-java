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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import parquet.column.Encoding;
import parquet.column.statistics.BinaryStatistics;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ParquetDecodingException;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

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
  public void testFooterCacheValueIsCurrent() throws IOException, InterruptedException {
    File tempFile = getTempFile();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    ParquetInputFormat.FootersCacheValue cacheValue = getDummyCacheValue(tempFile, fs);

    assertTrue(tempFile.setLastModified(tempFile.lastModified() + 5000));
    assertFalse(cacheValue.isCurrent(fs.getFileStatus(new Path(tempFile.getAbsolutePath()))));
  }

  @Test
  public void testFooterCacheValueIsNewer() throws IOException {
    File tempFile = getTempFile();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    ParquetInputFormat.FootersCacheValue cacheValue = getDummyCacheValue(tempFile, fs);

    assertTrue(cacheValue.isNewerThan(null));
    assertFalse(cacheValue.isNewerThan(cacheValue));

    assertTrue(tempFile.setLastModified(tempFile.lastModified() + 5000));
    ParquetInputFormat.FootersCacheValue newerCacheValue = getDummyCacheValue(tempFile, fs);

    assertTrue(newerCacheValue.isNewerThan(cacheValue));
    assertFalse(cacheValue.isNewerThan(newerCacheValue));
  }

  private File getTempFile() throws IOException {
    File tempFile = File.createTempFile("footer_", ".txt");
    tempFile.deleteOnExit();
    return tempFile;
  }

  private ParquetInputFormat.FootersCacheValue getDummyCacheValue(File file, FileSystem fs) throws IOException {
    Path path = new Path(file.getPath());
    FileStatus status = fs.getFileStatus(path);
    ParquetMetadata mockMetadata = mock(ParquetMetadata.class);
    ParquetInputFormat.FootersCacheValue cacheValue = new ParquetInputFormat.FootersCacheValue(status, new Footer(path, mockMetadata));
    assertTrue(cacheValue.isCurrent(status));
    return cacheValue;
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
