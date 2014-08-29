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

import static parquet.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import parquet.Log;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.filter2.compat.RowGroupFilter;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.GlobalMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ConfigurationUtil;
import parquet.hadoop.util.ContextUtil;
import parquet.hadoop.util.SerializationUtil;
import parquet.io.ParquetDecodingException;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * The input format to read a Parquet file.
 *
 * It requires an implementation of {@link ReadSupport} to materialize the records.
 *
 * The requestedSchema will control how the original records get projected by the loader.
 * It must be a subset of the original schema. Only the columns needed to reconstruct the records with the requestedSchema will be scanned.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class ParquetInputFormat<T> extends FileInputFormat<Void, T> {

  private static final Log LOG = Log.getLog(ParquetInputFormat.class);

  /**
   * key to configure the ReadSupport implementation
   */
  public static final String READ_SUPPORT_CLASS = "parquet.read.support.class";

  /**
   * key to configure the filter
   */
  public static final String UNBOUND_RECORD_FILTER = "parquet.read.filter";

  /**
   * key to configure type checking for conflicting schemas (default: true)
   */
  public static final String STRICT_TYPE_CHECKING = "parquet.strict.typing";

  /**
   * key to configure the filter predicate
   */
  public static final String FILTER_PREDICATE = "parquet.private.read.filter.predicate";

  public static final String TASK_SIDE_METADATA = "parquet.task.side.metadata";

  private static final int MIN_FOOTER_CACHE_SIZE = 100;

  public static void setTaskSideMetaData(Job job,  boolean taskSideMetadata) {
    ContextUtil.getConfiguration(job).setBoolean(TASK_SIDE_METADATA, taskSideMetadata);
  }

  public static boolean isTaskSideMetaData(Configuration configuration) {
    return configuration.getBoolean(TASK_SIDE_METADATA, Boolean.FALSE);
  }

  public static void setReadSupportClass(Job job,  Class<?> readSupportClass) {
    ContextUtil.getConfiguration(job).set(READ_SUPPORT_CLASS, readSupportClass.getName());
  }

  public static void setUnboundRecordFilter(Job job, Class<? extends UnboundRecordFilter> filterClass) {
    Configuration conf = ContextUtil.getConfiguration(job);
    checkArgument(getFilterPredicate(conf) == null,
        "You cannot provide an UnboundRecordFilter after providing a FilterPredicate");

    conf.set(UNBOUND_RECORD_FILTER, filterClass.getName());
  }

  /**
   * @deprecated use {@link #getFilter(Configuration)}
   */
  @Deprecated
  public static Class<?> getUnboundRecordFilter(Configuration configuration) {
    return ConfigurationUtil.getClassFromConfig(configuration, UNBOUND_RECORD_FILTER, UnboundRecordFilter.class);
  }

  private static UnboundRecordFilter getUnboundRecordFilterInstance(Configuration configuration) {
    Class<?> clazz = ConfigurationUtil.getClassFromConfig(configuration, UNBOUND_RECORD_FILTER, UnboundRecordFilter.class);
    if (clazz == null) { return null; }

    try {
      UnboundRecordFilter unboundRecordFilter = (UnboundRecordFilter) clazz.newInstance();

      if (unboundRecordFilter instanceof Configurable) {
        ((Configurable)unboundRecordFilter).setConf(configuration);
      }

      return unboundRecordFilter;
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instantiate unbound record filter class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate unbound record filter class", e);
    }
  }

  public static void setReadSupportClass(JobConf conf, Class<?> readSupportClass) {
    conf.set(READ_SUPPORT_CLASS, readSupportClass.getName());
  }

  public static Class<?> getReadSupportClass(Configuration configuration) {
    return ConfigurationUtil.getClassFromConfig(configuration, READ_SUPPORT_CLASS, ReadSupport.class);
  }

  public static void setFilterPredicate(Configuration configuration, FilterPredicate filterPredicate) {
    checkArgument(getUnboundRecordFilter(configuration) == null,
        "You cannot provide a FilterPredicate after providing an UnboundRecordFilter");

    configuration.set(FILTER_PREDICATE + ".human.readable", filterPredicate.toString());
    try {
      SerializationUtil.writeObjectToConfAsBase64(FILTER_PREDICATE, filterPredicate, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static FilterPredicate getFilterPredicate(Configuration configuration) {
    try {
      return SerializationUtil.readObjectFromConfAsBase64(FILTER_PREDICATE, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a non-null Filter, which is a wrapper around either a
   * FilterPredicate, an UnboundRecordFilter, or a no-op filter.
   */
  public static Filter getFilter(Configuration conf) {
    return FilterCompat.get(getFilterPredicate(conf), getUnboundRecordFilterInstance(conf));
  }

  private LruCache<FileStatusWrapper, FootersCacheValue> footersCache;

  private Class<?> readSupportClass;

  /**
   * Hadoop will instantiate using this constructor
   */
  public ParquetInputFormat() {
  }

  /**
   * constructor used when this InputFormat in wrapped in another one (In Pig for example)
   * @param readSupportClass the class to materialize records
   */
  public <S extends ReadSupport<T>> ParquetInputFormat(Class<S> readSupportClass) {
    this.readSupportClass = readSupportClass;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);
    ReadSupport<T> readSupport = getReadSupport(conf);
    return new ParquetRecordReader<T>(readSupport, getFilter(conf));
  }

  /**
   * @param configuration to find the configuration for the read support
   * @return the configured read support
   */
  public ReadSupport<T> getReadSupport(Configuration configuration){
    try {
      if (readSupportClass == null) {
        // TODO: fix this weird caching independent of the conf parameter
        readSupportClass = getReadSupportClass(configuration);
      }
      return (ReadSupport<T>)readSupportClass.newInstance();
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration configuration = ContextUtil.getConfiguration(jobContext);
    return new ArrayList<InputSplit>(getSplits(configuration, getFooters(jobContext)));
  }

  /**
   * @param configuration the configuration to connect to the file system
   * @param footers the footers of the files to read
   * @return the splits for the footers
   * @throws IOException
   */
  public List<ParquetInputSplit> getSplits(Configuration configuration, List<Footer> footers) throws IOException {
    boolean taskSideMetaData = isTaskSideMetaData(configuration);
    boolean strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
    final long maxSplitSize = configuration.getLong("mapred.max.split.size", Long.MAX_VALUE);
    final long minSplitSize = Math.max(getFormatMinSplitSize(), configuration.getLong("mapred.min.split.size", 0L));
    if (maxSplitSize < 0 || minSplitSize < 0) {
      throw new ParquetDecodingException("maxSplitSize or minSplitSize should not be negative: maxSplitSize = " + maxSplitSize + "; minSplitSize = " + minSplitSize);
    }
    GlobalMetaData globalMetaData = ParquetFileWriter.getGlobalMetaData(footers, strictTypeChecking);
    ReadContext readContext = getReadSupport(configuration).init(new InitContext(
        configuration,
        globalMetaData.getKeyValueMetaData(),
        globalMetaData.getSchema()));

    return SplitStrategy.getSplitStrategy(taskSideMetaData).getSplits(configuration, footers, maxSplitSize, minSplitSize, readContext);
  }

  /*
   * This is to support multi-level/recursive directory listing until
   * MAPREDUCE-1577 is fixed.
   */
  @Override
  protected List<FileStatus> listStatus(JobContext jobContext) throws IOException {
    return getAllFileRecursively(super.listStatus(jobContext),
       ContextUtil.getConfiguration(jobContext));
  }

  private static List<FileStatus> getAllFileRecursively(
      List<FileStatus> files, Configuration conf) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    for (FileStatus file : files) {
      if (file.isDir()) {
        Path p = file.getPath();
        FileSystem fs = p.getFileSystem(conf);
        staticAddInputPathRecursively(result, fs, p, hiddenFileFilter);
      } else {
        result.add(file);
      }
    }
    LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  private static void staticAddInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter)
          throws IOException {
    for (FileStatus stat: fs.listStatus(path, inputFilter)) {
      if (stat.isDir()) {
        staticAddInputPathRecursively(result, fs, stat.getPath(), inputFilter);
      } else {
        result.add(stat);
      }
    }
  }

  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * @param jobContext the current job context
   * @return the footers for the files
   * @throws IOException
   */
  public List<Footer> getFooters(JobContext jobContext) throws IOException {
    List<FileStatus> statuses = listStatus(jobContext);
    if (statuses.isEmpty()) {
      return Collections.emptyList();
    }
    Configuration config = ContextUtil.getConfiguration(jobContext);
    List<Footer> footers = new ArrayList<Footer>(statuses.size());
    Set<FileStatus> missingStatuses = new HashSet<FileStatus>();
    Map<Path, FileStatusWrapper> missingStatusesMap =
            new HashMap<Path, FileStatusWrapper>(missingStatuses.size());

    if (footersCache == null) {
      footersCache =
              new LruCache<FileStatusWrapper, FootersCacheValue>(Math.max(statuses.size(), MIN_FOOTER_CACHE_SIZE));
    }
    for (FileStatus status : statuses) {
      FileStatusWrapper statusWrapper = new FileStatusWrapper(status);
      FootersCacheValue cacheEntry =
              footersCache.getCurrentValue(statusWrapper);
      if (Log.DEBUG) {
        LOG.debug("Cache entry " + (cacheEntry == null ? "not " : "")
                + " found for '" + status.getPath() + "'");
      }
      if (cacheEntry != null) {
        footers.add(cacheEntry.getFooter());
      } else {
        missingStatuses.add(status);
        missingStatusesMap.put(status.getPath(), statusWrapper);
      }
    }
    if (Log.DEBUG) {
      LOG.debug("found " + footers.size() + " footers in cache and adding up "
              + "to " + missingStatuses.size() + " missing footers to the cache");
    }


    if (missingStatuses.isEmpty()) {
      return footers;
    }

    List<Footer> newFooters = getFooters(config, missingStatuses);
    for (Footer newFooter : newFooters) {
      // Use the original file status objects to make sure we store a
      // conservative (older) modification time (i.e. in case the files and
      // footers were modified and it's not clear which version of the footers
      // we have)
      FileStatusWrapper fileStatus = missingStatusesMap.get(newFooter.getFile());
      footersCache.put(fileStatus, new FootersCacheValue(fileStatus, newFooter));
    }

    footers.addAll(newFooters);
    return footers;
  }

  public List<Footer> getFooters(Configuration configuration, List<FileStatus> statuses) throws IOException {
    return getFooters(configuration, (Collection<FileStatus>)statuses);
  }

  /**
   * the footers for the files
   * @param configuration to connect to the file system
   * @param statuses the files to open
   * @return the footers of the files
   * @throws IOException
   */
  public List<Footer> getFooters(Configuration configuration, Collection<FileStatus> statuses) throws IOException {
    if (Log.DEBUG) LOG.debug("reading " + statuses.size() + " files");
    boolean taskSideMetaData = isTaskSideMetaData(configuration);
    return ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses, taskSideMetaData);
  }

  /**
   * @param jobContext the current job context
   * @return the merged metadata from the footers
   * @throws IOException
   */
  public GlobalMetaData getGlobalMetaData(JobContext jobContext) throws IOException {
    return ParquetFileWriter.getGlobalMetaData(getFooters(jobContext));
  }

  /**
   * A simple wrapper around {@link parquet.hadoop.Footer} that also includes a
   * modification time associated with that footer.  The modification time is
   * used to determine whether the footer is still current.
   */
  static final class FootersCacheValue
          implements LruCache.Value<FileStatusWrapper, FootersCacheValue> {
    private final long modificationTime;
    private final Footer footer;

    public FootersCacheValue(FileStatusWrapper status, Footer footer) {
      this.modificationTime = status.getModificationTime();
      this.footer = new Footer(footer.getFile(), footer.getParquetMetadata());
    }

    @Override
    public boolean isCurrent(FileStatusWrapper key) {
      long currentModTime = key.getModificationTime();
      boolean isCurrent = modificationTime >= currentModTime;
      if (Log.DEBUG && !isCurrent) {
        LOG.debug("The cache value for '" + key + "' is not current: "
                + "cached modification time=" + modificationTime + ", "
                + "current modification time: " + currentModTime);
      }
      return isCurrent;
    }

    public Footer getFooter() {
      return footer;
    }

    @Override
    public boolean isNewerThan(FootersCacheValue otherValue) {
      return otherValue == null ||
              modificationTime > otherValue.modificationTime;
    }

    public Path getPath() {
      return footer.getFile();
    }
  }

  /**
   * A simple wrapper around {@link org.apache.hadoop.fs.FileStatus} with a
   * meaningful "toString()" method
   */
  static final class FileStatusWrapper {
    private final FileStatus status;
    public FileStatusWrapper(FileStatus fileStatus) {
      if (fileStatus == null) {
        throw new IllegalArgumentException("FileStatus object cannot be null");
      }
      status = fileStatus;
    }

    public long getModificationTime() {
      return status.getModificationTime();
    }

    @Override
    public int hashCode() {
      return status.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof FileStatusWrapper &&
              status.equals(((FileStatusWrapper) other).status);
    }

    @Override
    public String toString() {
      return status.getPath().toString();
    }
  }

}
abstract class SplitStrategy {
  private static final Log LOG = Log.getLog(SplitStrategy.class);

  static SplitStrategy getSplitStrategy(boolean taskSideMetaData) {
    if (taskSideMetaData) {
      LOG.info("Using Task Side Metadata Split Strategy");
      return new TaskSideMetadataSplitStrategy();
    } else {
      LOG.info("Using Client Side Metadata Split Strategy");
      return new ClientSideMetadataSplitStrategy();
    }
  }

  abstract List<ParquetInputSplit> getSplits(
      Configuration configuration,
      List<Footer> footers,
      final long maxSplitSize, final long minSplitSize,
      ReadContext readContext) throws IOException;
}
class TaskSideMetadataSplitStrategy extends SplitStrategy {

  @Override
  List<ParquetInputSplit> getSplits(Configuration configuration, List<Footer> footers,
      long maxSplitSize, long minSplitSize, ReadContext readContext) throws IOException {
    List<ParquetInputSplit> splits = new ArrayList<ParquetInputSplit>();
    for (Footer footer : footers) {
      // TODO: keep status in Footer
      final Path file = footer.getFile();
      FileSystem fs = file.getFileSystem(configuration);
      FileStatus fileStatus = fs.getFileStatus(file);
      BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      splits.addAll(generateTaskSideMDSplits(
          fileBlockLocations,
          fileStatus,
          readContext.getRequestedSchema().toString(),
          readContext.getReadSupportMetadata(),
          minSplitSize,
          maxSplitSize));

    }
    return splits;
  }

  private static int findBlockIndex(BlockLocation[] hdfsBlocksArray, long offset) {
    for (int i = 0; i < hdfsBlocksArray.length; i++) {
      BlockLocation block = hdfsBlocksArray[i];
      if (offset >= block.getOffset() && offset < (block.getOffset() + block.getLength())) {
        return i;
      }
    }
    return -1;
  }

  static <T> List<ParquetInputSplit> generateTaskSideMDSplits(
      BlockLocation[] hdfsBlocksArray,
      FileStatus fileStatus,
      String requestedSchema,
      Map<String, String> readSupportMetadata, long minSplitSize, long maxSplitSize) throws IOException {
    if (maxSplitSize < minSplitSize || maxSplitSize < 0 || minSplitSize < 0) {
      throw new ParquetDecodingException("maxSplitSize and minSplitSize should be positive and max should be greater or equal to the minSplitSize: maxSplitSize = " + maxSplitSize + "; minSplitSize is " + minSplitSize);
    }
    //generate splits from rowGroups of each split
    List<ParquetInputSplit> resultSplits = new ArrayList<ParquetInputSplit>();
    // [startOffset, endOffset)
    long startOffset = 0;
    long endOffset = 0;
    // they should already be sorted
    Arrays.sort(hdfsBlocksArray, new Comparator<BlockLocation>() {
      @Override
      public int compare(BlockLocation o1, BlockLocation o2) {
        return compare(o1.getOffset(), o2.getOffset());
      }
      private int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
      }
    });
    final BlockLocation lastBlock =
        hdfsBlocksArray.length == 0 ? null : hdfsBlocksArray[hdfsBlocksArray.length - 1];
    while (endOffset < fileStatus.getLen()) {
      startOffset = endOffset;
      BlockLocation blockLocation;
      final int nextBlockMin = findBlockIndex(hdfsBlocksArray, startOffset + minSplitSize);
      final int nextBlockMax = findBlockIndex(hdfsBlocksArray, startOffset + maxSplitSize);
      if (nextBlockMax == nextBlockMin && nextBlockMax != -1) {
        // no block boundary between min and max
        // => use max for the size of the split
        endOffset = startOffset + maxSplitSize;
        blockLocation = hdfsBlocksArray[nextBlockMax];
      } else if (nextBlockMin > -1) {
        // block boundary between min and max
        // we end the split at the first block boundary
        blockLocation = hdfsBlocksArray[nextBlockMin];
        endOffset = blockLocation.getOffset() + blockLocation.getLength();
      } else {
        // min and max after last block
        // small last split
        endOffset = fileStatus.getLen();
        blockLocation = lastBlock;
      }
      resultSplits.add(
          new ParquetInputSplit(
              fileStatus.getPath(),
              startOffset, endOffset, endOffset - startOffset,
              blockLocation == null ? new String[0] : blockLocation.getHosts(),
              null,
              requestedSchema, readSupportMetadata));
    }
    return resultSplits;
  }
}
class ClientSideMetadataSplitStrategy extends SplitStrategy {
  //Wrapper of hdfs blocks, keep track of which HDFS block is being used
  private static class HDFSBlocks {
    BlockLocation[] hdfsBlocks;
    int currentStartHdfsBlockIndex = 0;//the hdfs block index corresponding to the start of a row group
    int currentMidPointHDFSBlockIndex = 0;// the hdfs block index corresponding to the mid-point of a row group, a split might be created only when the midpoint of the rowgroup enters a new hdfs block

    private HDFSBlocks(BlockLocation[] hdfsBlocks) {
      this.hdfsBlocks = hdfsBlocks;
      Comparator<BlockLocation> comparator = new Comparator<BlockLocation>() {
        @Override
        public int compare(BlockLocation b1, BlockLocation b2) {
          return Long.signum(b1.getOffset() - b2.getOffset());
        }
      };
      Arrays.sort(hdfsBlocks, comparator);
    }

    private long getHDFSBlockEndingPosition(int hdfsBlockIndex) {
      BlockLocation hdfsBlock = hdfsBlocks[hdfsBlockIndex];
      return hdfsBlock.getOffset() + hdfsBlock.getLength() - 1;
    }

    /**
     * @param rowGroupMetadata
     * @return true if the mid point of row group is in a new hdfs block, and also move the currentHDFSBlock pointer to the correct index that contains the row group;
     * return false if the mid point of row group is in the same hdfs block
     */
    private boolean checkBelongingToANewHDFSBlock(BlockMetaData rowGroupMetadata) {
      boolean isNewHdfsBlock = false;
      long rowGroupMidPoint = rowGroupMetadata.getStartingPos() + (rowGroupMetadata.getCompressedSize() / 2);

      //if mid point is not in the current HDFS block any more, return true
      while (rowGroupMidPoint > getHDFSBlockEndingPosition(currentMidPointHDFSBlockIndex)) {
        isNewHdfsBlock = true;
        currentMidPointHDFSBlockIndex++;
        if (currentMidPointHDFSBlockIndex >= hdfsBlocks.length)
          throw new ParquetDecodingException("the row group is not in hdfs blocks in the file: midpoint of row groups is "
                  + rowGroupMidPoint
                  + ", the end of the hdfs block is "
                  + getHDFSBlockEndingPosition(currentMidPointHDFSBlockIndex - 1));
      }

      while (rowGroupMetadata.getStartingPos() > getHDFSBlockEndingPosition(currentStartHdfsBlockIndex)) {
        currentStartHdfsBlockIndex++;
        if (currentStartHdfsBlockIndex >= hdfsBlocks.length)
          throw new ParquetDecodingException("The row group does not start in this file: row group offset is "
                  + rowGroupMetadata.getStartingPos()
                  + " but the end of hdfs blocks of file is "
                  + getHDFSBlockEndingPosition(currentStartHdfsBlockIndex));
      }
      return isNewHdfsBlock;
    }

    public BlockLocation getCurrentBlock() {
      return hdfsBlocks[currentStartHdfsBlockIndex];
    }
  }

  static class SplitInfo {
    List<BlockMetaData> rowGroups = new ArrayList<BlockMetaData>();
    BlockLocation hdfsBlock;
    long compressedByteSize = 0L;

    public SplitInfo(BlockLocation currentBlock) {
      this.hdfsBlock = currentBlock;
    }

    private void addRowGroup(BlockMetaData rowGroup) {
      this.rowGroups.add(rowGroup);
      this.compressedByteSize += rowGroup.getCompressedSize();
    }

    public long getCompressedByteSize() {
      return compressedByteSize;
    }

    public List<BlockMetaData> getRowGroups() {
      return rowGroups;
    }

    int getRowGroupCount() {
      return rowGroups.size();
    }

    public ParquetInputSplit getParquetInputSplit(FileStatus fileStatus, String requestedSchema, Map<String, String> readSupportMetadata) throws IOException {
      MessageType requested = MessageTypeParser.parseMessageType(requestedSchema);
      long length = 0;

      for (BlockMetaData block : this.getRowGroups()) {
        List<ColumnChunkMetaData> columns = block.getColumns();
        for (ColumnChunkMetaData column : columns) {
          if (requested.containsPath(column.getPath().toArray())) {
            length += column.getTotalSize();
          }
        }
      }

      BlockMetaData lastRowGroup = this.getRowGroups().get(this.getRowGroupCount() - 1);
      long end = lastRowGroup.getStartingPos() + lastRowGroup.getTotalByteSize();

      long[] rowGroupOffsets = new long[this.getRowGroupCount()];
      for (int i = 0; i < rowGroupOffsets.length; i++) {
        rowGroupOffsets[i] = this.getRowGroups().get(i).getStartingPos();
      }

      return new ParquetInputSplit(
              fileStatus.getPath(),
              hdfsBlock.getOffset(),
              end,
              length,
              hdfsBlock.getHosts(),
              rowGroupOffsets,
              requestedSchema,
              readSupportMetadata
      );
    }
  }

  private static final Log LOG = Log.getLog(ClientSideMetadataSplitStrategy.class);

  @Override
  List<ParquetInputSplit> getSplits(Configuration configuration, List<Footer> footers,
      long maxSplitSize, long minSplitSize, ReadContext readContext)
      throws IOException {
    List<ParquetInputSplit> splits = new ArrayList<ParquetInputSplit>();
    Filter filter = ParquetInputFormat.getFilter(configuration);

    long rowGroupsDropped = 0;
    long totalRowGroups = 0;

    for (Footer footer : footers) {
      final Path file = footer.getFile();
      LOG.debug(file);
      FileSystem fs = file.getFileSystem(configuration);
      FileStatus fileStatus = fs.getFileStatus(file);
      ParquetMetadata parquetMetaData = footer.getParquetMetadata();
      List<BlockMetaData> blocks = parquetMetaData.getBlocks();

      List<BlockMetaData> filteredBlocks;

      totalRowGroups += blocks.size();
      filteredBlocks = RowGroupFilter.filterRowGroups(filter, blocks, parquetMetaData.getFileMetaData().getSchema());
      rowGroupsDropped += blocks.size() - filteredBlocks.size();

      if (filteredBlocks.isEmpty()) {
        continue;
      }

      BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      splits.addAll(
          generateSplits(
              filteredBlocks,
              fileBlockLocations,
              fileStatus,
              readContext.getRequestedSchema().toString(),
              readContext.getReadSupportMetadata(),
              minSplitSize,
              maxSplitSize)
          );
    }

    if (rowGroupsDropped > 0 && totalRowGroups > 0) {
      int percentDropped = (int) ((((double) rowGroupsDropped) / totalRowGroups) * 100);
      LOG.info("Dropping " + rowGroupsDropped + " row groups that do not pass filter predicate! (" + percentDropped + "%)");
    } else {
      LOG.info("There were no row groups that could be dropped due to filter predicates");
    }
    return splits;
  }

  /**
   * groups together all the data blocks for the same HDFS block
   *
   * @param rowGroupBlocks      data blocks (row groups)
   * @param hdfsBlocksArray     hdfs blocks
   * @param fileStatus          the containing file
   * @param requestedSchema     the schema requested by the user
   * @param readSupportMetadata the metadata provided by the readSupport implementation in init
   * @param minSplitSize        the mapred.min.split.size
   * @param maxSplitSize        the mapred.max.split.size
   * @return the splits (one per HDFS block)
   * @throws IOException If hosts can't be retrieved for the HDFS block
   */
  static <T> List<ParquetInputSplit> generateSplits(
          List<BlockMetaData> rowGroupBlocks,
          BlockLocation[] hdfsBlocksArray,
          FileStatus fileStatus,
          String requestedSchema,
          Map<String, String> readSupportMetadata, long minSplitSize, long maxSplitSize) throws IOException {

    List<SplitInfo> splitRowGroups =
        generateSplitInfo(rowGroupBlocks, hdfsBlocksArray, minSplitSize, maxSplitSize);

    //generate splits from rowGroups of each split
    List<ParquetInputSplit> resultSplits = new ArrayList<ParquetInputSplit>();
    for (SplitInfo splitInfo : splitRowGroups) {
      ParquetInputSplit split = splitInfo.getParquetInputSplit(fileStatus, requestedSchema, readSupportMetadata);
      resultSplits.add(split);
    }
    return resultSplits;
  }

  static List<SplitInfo> generateSplitInfo(
      List<BlockMetaData> rowGroupBlocks,
      BlockLocation[] hdfsBlocksArray,
      long minSplitSize, long maxSplitSize) {
    List<SplitInfo> splitRowGroups;

    if (maxSplitSize < minSplitSize || maxSplitSize < 0 || minSplitSize < 0) {
      throw new ParquetDecodingException("maxSplitSize and minSplitSize should be positive and max should be greater or equal to the minSplitSize: maxSplitSize = " + maxSplitSize + "; minSplitSize is " + minSplitSize);
    }
    HDFSBlocks hdfsBlocks = new HDFSBlocks(hdfsBlocksArray);
    hdfsBlocks.checkBelongingToANewHDFSBlock(rowGroupBlocks.get(0));
    SplitInfo currentSplit = new SplitInfo(hdfsBlocks.getCurrentBlock());

    //assign rowGroups to splits
    splitRowGroups = new ArrayList<SplitInfo>();
    checkSorted(rowGroupBlocks);//assert row groups are sorted
    for (BlockMetaData rowGroupMetadata : rowGroupBlocks) {
      if ((hdfsBlocks.checkBelongingToANewHDFSBlock(rowGroupMetadata)
             && currentSplit.getCompressedByteSize() >= minSplitSize
             && currentSplit.getCompressedByteSize() > 0)
           || currentSplit.getCompressedByteSize() >= maxSplitSize) {
        //create a new split
        splitRowGroups.add(currentSplit);//finish previous split
        currentSplit = new SplitInfo(hdfsBlocks.getCurrentBlock());
      }
      currentSplit.addRowGroup(rowGroupMetadata);
    }

    if (currentSplit.getRowGroupCount() > 0) {
      splitRowGroups.add(currentSplit);
    }

    return splitRowGroups;
  }

  private static void checkSorted(List<BlockMetaData> rowGroupBlocks) {
    long previousOffset = 0L;
    for(BlockMetaData rowGroup: rowGroupBlocks) {
      long currentOffset = rowGroup.getStartingPos();
      if (currentOffset < previousOffset) {
        throw new ParquetDecodingException("row groups are not sorted: previous row groups starts at " + previousOffset + ", current row group starts at " + currentOffset);
      }
    }
  }
}