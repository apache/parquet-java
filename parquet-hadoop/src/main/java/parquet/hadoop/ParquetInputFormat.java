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
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import parquet.Log;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.parser.MessageTypeParser;
import parquet.schema.MessageType;

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
    job.getConfiguration().set(READ_SUPPORT_CLASS, readSupportClass.getName());
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

  private String requestedSchema;
  private Class<?> readSupportClass;

  private List<Footer> footers;

  public ParquetInputFormat() {
  }

  public <S extends ReadSupport<T>> ParquetInputFormat(Class<S> readSupportClass) {
    this.readSupportClass = readSupportClass;
  }

  /**
   * constructor used when this InputFormat in wrapped in another one (In Pig for example)
   * @param readSupportClass the class to materialize records
   * @param requestedSchema the schema use to project the records (must be a subset of the original schema)
   */
  public <S extends ReadSupport<T>> ParquetInputFormat(Class<S> readSupportClass, String requestedSchema) {
    this.readSupportClass = readSupportClass;
    this.requestedSchema = requestedSchema;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    @SuppressWarnings("unchecked") // I know
    ParquetInputSplit<T> parquetInputSplit = (ParquetInputSplit<T>)inputSplit;
    if (readSupportClass == null) {
      readSupportClass = getReadSupportClass(taskAttemptContext.getConfiguration());
    }
    return new ParquetRecordReader<T>(getRequestedSchema(parquetInputSplit.getSchema()), readSupportClass);
  }

  private String getRequestedSchema(String fileSchema) {
    if (requestedSchema != null) {
      MessageType requestedMessageType = MessageTypeParser.parseMessageType(requestedSchema);
      MessageType fileMessageType = MessageTypeParser.parseMessageType(fileSchema);
      fileMessageType.checkContains(requestedMessageType);
      return requestedSchema;
    }
    return fileSchema;
  }

  /**
   * groups together all the data blocks for the same HDFS block
   * @param blocks data blocks (row groups)
   * @param hdfsBlocks hdfs blocks
   * @param fileStatus the containing file
   * @param fileMetaData file level meta data
   * @param extraMetadata
   * @param readSupport how to materialize the records
   * @return the splits (one per HDFS block)
   * @throws IOException If hosts can't be retrieved for the HDFS block
   */
  static <T> List<InputSplit> generateSplits(List<BlockMetaData> blocks,
      BlockLocation[] hdfsBlocks, FileStatus fileStatus,
      FileMetaData fileMetaData, Class<?> readSupportClass, String requestedSchema, Map<String, String> extraMetadata) throws IOException {
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
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < hdfsBlocks.length; i++) {
      BlockLocation hdfsBlock = hdfsBlocks[i];
      List<BlockMetaData> blocksForCurrentSplit = splitGroups.get(i);
      if (blocksForCurrentSplit.size() == 0) {
        LOG.warn("HDFS block without row group: " + hdfsBlocks[i]);
      } else {
        splits.add(new ParquetInputSplit<T>(
          fileStatus.getPath(),
          hdfsBlock.getOffset(),
          hdfsBlock.getLength(),
          hdfsBlock.getHosts(),
          blocksForCurrentSplit,
          fileMetaData.getSchema().toString(),
          readSupportClass,
          requestedSchema,
          extraMetadata
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
    Configuration configuration = jobContext.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    List<Footer> footers = getFooters(jobContext);
    for (Footer footer : footers) {
      LOG.debug(footer.getFile());
      FileStatus fileStatus = fs.getFileStatus(footer.getFile());
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
              requestedSchema,
              parquetMetaData.getKeyValueMetaData())
          );
    }
    return splits;
  }

  public List<Footer> getFooters(JobContext jobContext) throws IOException {
    if (footers == null) {
      Configuration configuration = jobContext.getConfiguration();
      List<FileStatus> statuses = super.listStatus(jobContext);
      LOG.debug("reading " + statuses.size() + " files");
      footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses);
    }
    return footers;
  }

}
