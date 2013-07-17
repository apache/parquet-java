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

import static parquet.hadoop.ParquetFileWriter.mergeInto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
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

  public static final String READ_SUPPORT_CLASS = "parquet.read.support.class";

  public static void setReadSupportClass(Job job,  Class<?> readSupportClass) {
    ContextUtil.getConfiguration(job).set(READ_SUPPORT_CLASS, readSupportClass.getName());
  }

  public static void setReadSupportClass(JobConf conf, Class<?> readSupportClass) {
    conf.set(READ_SUPPORT_CLASS, readSupportClass.getName());
  }

  public static Class<?> getReadSupportClass(Configuration configuration) {
    final String className = configuration.get(READ_SUPPORT_CLASS);
    if (className == null) {
      return null;
    }
    try {
      final Class<?> readSupportClass = Class.forName(className);
      if (!ReadSupport.class.isAssignableFrom(readSupportClass)) {
        throw new BadConfigurationException("class " + className + " set in job conf at " + READ_SUPPORT_CLASS + " is not a subclass of ReadSupport");
      }
      return readSupportClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("could not instanciate class " + className + " set in job conf at " + READ_SUPPORT_CLASS , e);
    }
  }

  private Class<?> readSupportClass;

  private List<Footer> footers;

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
    return new ParquetRecordReader<T>(getReadSupport(ContextUtil.getConfiguration(taskAttemptContext)));
  }

  public ReadSupport<T> getReadSupport(Configuration configuration){
    try {
      if (readSupportClass == null) {
        readSupportClass = getReadSupportClass(configuration);
      }
      @SuppressWarnings("unchecked") // I know
      ReadSupport<T> readSupport = (ReadSupport<T>)readSupportClass.newInstance();
      return readSupport;
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instanciate read support class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instanciate read support class", e);
    }
  }

  /**
   * groups together all the data blocks for the same HDFS block
   * @param blocks data blocks (row groups)
   * @param hdfsBlocks hdfs blocks
   * @param fileStatus the containing file
   * @param fileMetaData file level meta data
   * @param readSupportClass the class used to materialize records
   * @param requestedSchema the schema requested by the user
   * @param readSupportMetadata the metadata provided by the readSupport implementation in init
   * @return the splits (one per HDFS block)
   * @throws IOException If hosts can't be retrieved for the HDFS block
   */
  static <T> List<ParquetInputSplit> generateSplits(
      List<BlockMetaData> blocks,
      BlockLocation[] hdfsBlocks,
      FileStatus fileStatus,
      FileMetaData fileMetaData,
      Class<?> readSupportClass,
      String requestedSchema,
      Map<String, String> readSupportMetadata) throws IOException {
    Comparator<BlockLocation> comparator = new Comparator<BlockLocation>() {
      @Override
      public int compare(BlockLocation b1, BlockLocation b2) {
        return Long.signum(b1.getOffset() - b2.getOffset());
      }
    };
    Arrays.sort(hdfsBlocks, comparator);
    List<List<BlockMetaData>> splitGroups = new ArrayList<List<BlockMetaData>>(hdfsBlocks.length);
    for (int i = 0; i < hdfsBlocks.length; i++) {
      splitGroups.add(new ArrayList<BlockMetaData>());
    }
    for (BlockMetaData block : blocks) {
      final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
      int index = Arrays.binarySearch(hdfsBlocks, new BlockLocation() {@Override
        public long getOffset() {
        return firstDataPage;
      }}, comparator);
      if (index >= 0) {
        splitGroups.get(index).add(block);
      } else {
        int insertionPoint = - index - 1;
        if (insertionPoint == 0) {
          // really, there should always be a block in 0
          LOG.warn("row group before the first HDFS block:  " + block);
          splitGroups.get(0).add(block);
        } else {
          splitGroups.get(insertionPoint - 1).add(block);
        }
      }
    }
    List<ParquetInputSplit> splits = new ArrayList<ParquetInputSplit>();
    for (int i = 0; i < hdfsBlocks.length; i++) {
      BlockLocation hdfsBlock = hdfsBlocks[i];
      List<BlockMetaData> blocksForCurrentSplit = splitGroups.get(i);
      if (blocksForCurrentSplit.size() == 0) {
        LOG.warn("HDFS block without row group: " + hdfsBlocks[i]);
      } else {
        long length = 0;
        for (BlockMetaData block : blocksForCurrentSplit) {
          MessageType requested = MessageTypeParser.parseMessageType(requestedSchema);
          List<ColumnChunkMetaData> columns = block.getColumns();
          for (ColumnChunkMetaData column : columns) {
            if (requested.containsPath(column.getPath())) {
              length += column.getTotalSize();
            }
          }
        }
        splits.add(new ParquetInputSplit(
            fileStatus.getPath(),
            hdfsBlock.getOffset(),
            length,
            hdfsBlock.getHosts(),
            blocksForCurrentSplit,
            fileMetaData.getSchema().toString(),
            requestedSchema,
            fileMetaData.getSchema().toString(),
            fileMetaData.getKeyValueMetaData(),
            readSupportMetadata
            ));
      }
    }
    return splits;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    splits.addAll(getSplits(ContextUtil.getConfiguration(jobContext), getFooters(jobContext)));
    return splits;
  }

  public List<ParquetInputSplit> getSplits(Configuration configuration, List<Footer> footers) throws IOException {
    List<ParquetInputSplit> splits = new ArrayList<ParquetInputSplit>();
    FileMetaData globalMetaData = getGlobalMetaData(footers);
    ReadContext readContext = getReadSupport(configuration).init(
        configuration,
        globalMetaData.getKeyValueMetaData(),
        globalMetaData.getSchema());
    for (Footer footer : footers) {
      final Path file = footer.getFile();
      LOG.debug(file);
      FileSystem fs = file.getFileSystem(configuration);
      FileStatus fileStatus = fs.getFileStatus(file);
      ParquetMetadata parquetMetaData = footer.getParquetMetadata();
      List<BlockMetaData> blocks = parquetMetaData.getBlocks();
      BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      splits.addAll(
          generateSplits(
              blocks,
              fileBlockLocations,
              fileStatus,
              parquetMetaData.getFileMetaData(),
              readSupportClass,
              readContext.getRequestedSchema().toString(),
              readContext.getReadSupportMetadata())
          );
    }
    return splits;
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
    int len = files.size();
    for (int i = 0; i < len; ++i) {
      FileStatus file = files.get(i);
      if (file.isDir()) {
        Path p = file.getPath();
        FileSystem fs = p.getFileSystem(conf);
        addInputPathRecursively(result, fs, p, hiddenFileFilter);
      } else {
        result.add(file);
      }
    }
    LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  private static void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter)
          throws IOException {
    for (FileStatus stat: fs.listStatus(path, inputFilter)) {
      if (stat.isDir()) {
        addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
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
    if (footers == null) {
      footers = getFooters(ContextUtil.getConfiguration(jobContext), listStatus(jobContext));
    }

    return footers;
  }

  public List<Footer> getFooters(Configuration configuration, List<FileStatus> statuses) throws IOException {
    LOG.debug("reading " + statuses.size() + " files");
    return ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses);
  }

  /**
   * @param jobContext the current job context
   * @return the merged metadata from the footers
   * @throws IOException
   */
  public FileMetaData getGlobalMetaData(JobContext jobContext) throws IOException {
    return getGlobalMetaData(getFooters(jobContext));
  }

  private FileMetaData getGlobalMetaData(List<Footer> footers) throws IOException {
    FileMetaData fileMetaData = null;
    for (Footer footer : footers) {
      ParquetMetadata currentMetadata = footer.getParquetMetadata();
      fileMetaData = mergeInto(currentMetadata.getFileMetaData(), fileMetaData);
    }
    return fileMetaData;
  }
}
