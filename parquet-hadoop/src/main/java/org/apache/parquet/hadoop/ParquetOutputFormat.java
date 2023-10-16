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
package org.apache.parquet.hadoop;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_BLOOM_FILTER_ENABLED;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_ADAPTIVE_BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.util.ContextUtil.getConfiguration;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OutputFormat to write to a Parquet file
 * <p>
 * It requires a {@link WriteSupport} to convert the actual records to the underlying format.
 * It requires the schema of the incoming records. (provided by the write support)
 * It allows storing extra metadata in the footer (for example: for schema compatibility purpose when converting from a different schema language).
 * <p>
 * The format configuration settings in the job configuration:
 * <pre>
 * # The block size is the size of a row group being buffered in memory
 * # this limits the memory usage when writing
 * # Larger values will improve the IO when reading but consume more memory when writing
 * parquet.block.size=134217728 # in bytes, default = 128 * 1024 * 1024
 *
 * # The page size is for compression. When reading, each page can be decompressed independently.
 * # A block is composed of pages. The page is the smallest unit that must be read fully to access a single record.
 * # If this value is too small, the compression will deteriorate
 * parquet.page.size=1048576 # in bytes, default = 1 * 1024 * 1024
 *
 * # There is one dictionary page per column per row group when dictionary encoding is used.
 * # The dictionary page size works like the page size but for dictionary
 * parquet.dictionary.page.size=1048576 # in bytes, default = 1 * 1024 * 1024
 *
 * # The compression algorithm used to compress pages
 * parquet.compression=UNCOMPRESSED # one of: UNCOMPRESSED, SNAPPY, GZIP, LZO, ZSTD. Default: UNCOMPRESSED. Supersedes mapred.output.compress*
 *
 * # The write support class to convert the records written to the OutputFormat into the events accepted by the record consumer
 * # Usually provided by a specific ParquetOutputFormat subclass
 * parquet.write.support.class= # fully qualified name
 *
 * # To enable/disable dictionary encoding
 * parquet.enable.dictionary=true # false to disable dictionary encoding
 *
 * # To enable/disable summary metadata aggregation at the end of a MR job
 * # The default is true (enabled)
 * parquet.enable.summary-metadata=true # false to disable summary aggregation
 *
 * # Maximum size (in bytes) allowed as padding to align row groups
 * # This is also the minimum size of a row group. Default: 8388608
 * parquet.writer.max-padding=8388608 # 8 MB
 * </pre>
 *
 * If parquet.compression is not set, the following properties are checked (FileOutputFormat behavior).
 * Note that we explicitely disallow custom Codecs
 * <pre>
 * mapred.output.compress=true
 * mapred.output.compression.codec=org.apache.hadoop.io.compress.SomeCodec # the codec must be one of Snappy, GZip or LZO
 * </pre>
 *
 * if none of those is set the data is uncompressed.
 *
 * @param <T> the type of the materialized records
 */
public class ParquetOutputFormat<T> extends FileOutputFormat<Void, T> {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputFormat.class);

  public enum JobSummaryLevel {
    /**
     * Write no summary files
     */
    NONE,
    /**
     * Write both summary file with row group info and summary file without
     * (both _metadata and _common_metadata)
     */
    ALL,
    /**
     * Write only the summary file without the row group info
     * (_common_metadata only)
     */
    COMMON_ONLY
  }

  /**
   * An alias for JOB_SUMMARY_LEVEL, where true means ALL and false means NONE
   */
  @Deprecated
  public static final String ENABLE_JOB_SUMMARY   = "parquet.enable.summary-metadata";

  /**
   * Must be one of the values in {@link JobSummaryLevel} (case-insensitive)
   */
  public static final String JOB_SUMMARY_LEVEL = "parquet.summary.metadata.level";
  public static final String BLOCK_SIZE           = "parquet.block.size";
  public static final String PAGE_SIZE            = "parquet.page.size";
  public static final String COMPRESSION          = "parquet.compression";
  public static final String WRITE_SUPPORT_CLASS  = "parquet.write.support.class";
  public static final String DICTIONARY_PAGE_SIZE = "parquet.dictionary.page.size";
  public static final String ENABLE_DICTIONARY    = "parquet.enable.dictionary";
  public static final String VALIDATION           = "parquet.validation";
  public static final String WRITER_VERSION       = "parquet.writer.version";
  public static final String MEMORY_POOL_RATIO    = "parquet.memory.pool.ratio";
  public static final String MIN_MEMORY_ALLOCATION = "parquet.memory.min.chunk.size";
  public static final String MAX_PADDING_BYTES    = "parquet.writer.max-padding";
  public static final String MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK = "parquet.page.size.row.check.min";
  public static final String MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK = "parquet.page.size.row.check.max";
  public static final String PAGE_VALUE_COUNT_THRESHOLD = "parquet.page.value.count.threshold";
  public static final String ESTIMATE_PAGE_SIZE_CHECK = "parquet.page.size.check.estimate";
  public static final String COLUMN_INDEX_TRUNCATE_LENGTH = "parquet.columnindex.truncate.length";
  public static final String STATISTICS_TRUNCATE_LENGTH = "parquet.statistics.truncate.length";
  public static final String BLOOM_FILTER_ENABLED = "parquet.bloom.filter.enabled";
  public static final String BLOOM_FILTER_EXPECTED_NDV = "parquet.bloom.filter.expected.ndv";
  public static final String BLOOM_FILTER_MAX_BYTES = "parquet.bloom.filter.max.bytes";
  public static final String BLOOM_FILTER_FPP = "parquet.bloom.filter.fpp";
  public static final String ADAPTIVE_BLOOM_FILTER_ENABLED = "parquet.bloom.filter.adaptive.enabled";
  public static final String BLOOM_FILTER_CANDIDATES_NUMBER = "parquet.bloom.filter.candidates.number";
  public static final String PAGE_ROW_COUNT_LIMIT = "parquet.page.row.count.limit";
  public static final String PAGE_WRITE_CHECKSUM_ENABLED = "parquet.page.write-checksum.enabled";

  /**
   * Get the job summary level from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the job summary level set in the configuration
   */
  public static JobSummaryLevel getJobSummaryLevel(Configuration conf) {
    String level = conf.get(JOB_SUMMARY_LEVEL);
    String deprecatedFlag = conf.get(ENABLE_JOB_SUMMARY);

    if (deprecatedFlag != null) {
      LOG.warn("Setting " + ENABLE_JOB_SUMMARY + " is deprecated, please use " + JOB_SUMMARY_LEVEL);
    }

    if (level != null && deprecatedFlag != null) {
      LOG.warn("Both " + JOB_SUMMARY_LEVEL + " and " + ENABLE_JOB_SUMMARY + " are set! " + ENABLE_JOB_SUMMARY + " will be ignored.");
    }

    if (level != null) {
      return JobSummaryLevel.valueOf(level.toUpperCase());
    }

    if (deprecatedFlag != null) {
      return Boolean.parseBoolean(deprecatedFlag) ? JobSummaryLevel.ALL : JobSummaryLevel.NONE;
    }

    return JobSummaryLevel.ALL;
  }

  /**
   * Set the write support class in the {@link Job}s {@link Configuration}.
   *
   * @param job the job to set the write support class for
   * @param writeSupportClass the write support class to set
   */
  public static void setWriteSupportClass(Job job,  Class<?> writeSupportClass) {
    getConfiguration(job).set(WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  /**
   * Set the write support class in the {@link JobConf}.
   *
   * @param job the job configuration to set the write support class for
   * @param writeSupportClass the write support class to set
   */
  public static void setWriteSupportClass(JobConf job, Class<?> writeSupportClass) {
      job.set(WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  /**
   * Gets the write support class from a {@link Configuration}.
   *
   * @param configuration the configuration to get the write support class for
   */
  public static Class<?> getWriteSupportClass(Configuration configuration) {
    final String className = configuration.get(WRITE_SUPPORT_CLASS);
    if (className == null) {
      return null;
    }
    return ConfigurationUtil.getClassFromConfig(configuration, WRITE_SUPPORT_CLASS, WriteSupport.class);
  }

  /**
   * Sets the block size property in a {@link Job}'s {@link Configuration}.
   *
   * @param job the job to update the configuration of
   * @param blockSize the value to set the block size to
   */
  public static void setBlockSize(Job job, int blockSize) {
    getConfiguration(job).setInt(BLOCK_SIZE, blockSize);
  }

  /**
   * Sets the page size property in a {@link Job}'s {@link Configuration}.
   *
   * @param job the job to update the configuration of
   * @param pageSize the value to set the page size to
   */
  public static void setPageSize(Job job, int pageSize) {
    getConfiguration(job).setInt(PAGE_SIZE, pageSize);
  }

  /**
   * Sets the dictionary page size in a {@link Job}'s {@link Configuration}.
   *
   * @param job the job to update the configuration of
   * @param pageSize the value to set the dictionary page size to
   */
  public static void setDictionaryPageSize(Job job, int pageSize) {
    getConfiguration(job).setInt(DICTIONARY_PAGE_SIZE, pageSize);
  }

  /**
   * Sets the compression codec name in a {@link Job}'s {@link Configuration}.
   *
   * @param job the job to update the configuration of
   * @param compression the value to set the compression codec to
   */
  public static void setCompression(Job job, CompressionCodecName compression) {
    getConfiguration(job).set(COMPRESSION, compression.name());
  }

  /**
   * Sets the enable dictionary property in a {@link Job}'s {@link Configuration}.
   *
   * @param job the job to update the configuration of
   * @param enableDictionary the value to set the property to
   */
  public static void setEnableDictionary(Job job, boolean enableDictionary) {
    getConfiguration(job).setBoolean(ENABLE_DICTIONARY, enableDictionary);
  }

  /**
   * Check whether dictionary is enabled in a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return whether dictionary is enabled in a job context
   */
  public static boolean getEnableDictionary(JobContext jobContext) {
    return getEnableDictionary(getConfiguration(jobContext));
  }

  /**
   * Get the bloom filter max bytes property from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the value of the bloom filter max bytes property
   */
  public static int getBloomFilterMaxBytes(Configuration conf) {
    return conf.getInt(BLOOM_FILTER_MAX_BYTES,
      ParquetProperties.DEFAULT_MAX_BLOOM_FILTER_BYTES);
  }

  /**
   * Check whether bloom filter is enabled in a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return whether bloom filter is enabled
   */
  public static boolean getBloomFilterEnabled(Configuration conf) {
    return conf.getBoolean(BLOOM_FILTER_ENABLED, DEFAULT_BLOOM_FILTER_ENABLED);
  }

  /**
   * Check whether adaptive bloom filter is enabled in a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return whether adaptive bloom filter is enabled
   */
  public static boolean getAdaptiveBloomFilterEnabled(Configuration conf) {
    return conf.getBoolean(ADAPTIVE_BLOOM_FILTER_ENABLED, DEFAULT_ADAPTIVE_BLOOM_FILTER_ENABLED);
  }

  /**
   * Get the block size property from a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return the block size property of the job context's {@link Configuration}
   */
  public static int getBlockSize(JobContext jobContext) {
    return getBlockSize(getConfiguration(jobContext));
  }

  /**
   * Get the page size property from a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return the page size property of the job context's {@link Configuration}
   */
  public static int getPageSize(JobContext jobContext) {
    return getPageSize(getConfiguration(jobContext));
  }

  /**
   * Get the dictionary page property from a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return the dictionary page size property of the job context's {@link Configuration}
   */
  public static int getDictionaryPageSize(JobContext jobContext) {
    return getDictionaryPageSize(getConfiguration(jobContext));
  }

  /**
   * Get the Parquet compression from a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return the Parquet compression set in the job context's {@link Configuration}
   */
  public static CompressionCodecName getCompression(JobContext jobContext) {
    return getCompression(getConfiguration(jobContext));
  }

  /**
   * Check whether the Parquet compression is set for a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return whether the Parquet compression of the job context's {@link Configuration} is set
   */
  public static boolean isCompressionSet(JobContext jobContext) {
    return isCompressionSet(getConfiguration(jobContext));
  }

  /**
   * Sets the validation property in a {@link JobContext}.
   *
   * @param jobContext the context to update
   * @param validating the value to set the property to
   */
  public static void setValidation(JobContext jobContext, boolean validating) {
    setValidation(getConfiguration(jobContext), validating);
  }

  /**
   * Get the validation property from a {@link JobContext}.
   *
   * @param jobContext the job context to examine
   * @return the validation property of the job context's {@link Configuration}
   */
  public static boolean getValidation(JobContext jobContext) {
    return getValidation(getConfiguration(jobContext));
  }

  /**
   * Get the enable dictionary property from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the enable dictionary property of the configuration
   */
  public static boolean getEnableDictionary(Configuration configuration) {
    return configuration.getBoolean(
        ENABLE_DICTIONARY, ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED);
  }

  /**
   * Get the min row count for page size check property from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the min row count for page size check property of the configuration
   */
  public static int getMinRowCountForPageSizeCheck(Configuration configuration) {
    return configuration.getInt(MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK,
        ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK);
  }

  /**
   * Get the max row count for page size check property from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the max row count for page size check property of the configuration
   */
  public static int getMaxRowCountForPageSizeCheck(Configuration configuration) {
    return configuration.getInt(MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK,
        ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK);
  }

  /**
   * Get the value count threshold from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the value count threshold property of the configuration
   */
  public static int getValueCountThreshold(Configuration configuration) {
    return configuration.getInt(PAGE_VALUE_COUNT_THRESHOLD,
        ParquetProperties.DEFAULT_PAGE_VALUE_COUNT_THRESHOLD);
  }

  /**
   * Get the estimate page size check from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the estimate page size check property of the configuration
   */
  public static boolean getEstimatePageSizeCheck(Configuration configuration) {
    return configuration.getBoolean(ESTIMATE_PAGE_SIZE_CHECK,
        ParquetProperties.DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK);
  }

  /**
   * Get the block size from a {@link Configuration} as an integer.
   *
   * @deprecated use {@link ParquetOutputFormat#getLongBlockSize(Configuration)} instead
   *
   * @param configuration the configuration to examine
   * @return the block size property of the configuration as an integer
   */
  @Deprecated
  public static int getBlockSize(Configuration configuration) {
    return configuration.getInt(BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Get the block size from a {@link Configuration} as a long.
   *
   * @param configuration the configuration to examine
   * @return the block size property of the configuration as a long
   */
  public static long getLongBlockSize(Configuration configuration) {
    return configuration.getLong(BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Get the page size from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the page size property of the configuration
   */
  public static int getPageSize(Configuration configuration) {
    return configuration.getInt(PAGE_SIZE, ParquetProperties.DEFAULT_PAGE_SIZE);
  }

  /**
   * Get the dictionary page size from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the dictionary page size property of the configuration
   */
  public static int getDictionaryPageSize(Configuration configuration) {
    return configuration.getInt(
        DICTIONARY_PAGE_SIZE, ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
  }

  /**
   * Get the writer version from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the writer version set
   */
  public static WriterVersion getWriterVersion(Configuration configuration) {
    String writerVersion = configuration.get(
        WRITER_VERSION, ParquetProperties.DEFAULT_WRITER_VERSION.toString());
    return WriterVersion.fromString(writerVersion);
  }

  /**
   * Get the compression codec from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the Parquet compression codec set
   */
  public static CompressionCodecName getCompression(Configuration configuration) {
    return CodecConfig.getParquetCompressionCodec(configuration);
  }

  /**
   * Checks whether the Parquet compression is set in a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return whether the Parquet compression is set in the configuration
   */
  public static boolean isCompressionSet(Configuration configuration) {
    return CodecConfig.isParquetCompressionSet(configuration);
  }

  /**
   * Sets the validation property in a {@link Configuration}.
   *
   * @param configuration the configuration to update
   * @param validating the value to set the property to
   */
  public static void setValidation(Configuration configuration, boolean validating) {
    configuration.setBoolean(VALIDATION, validating);
  }

  /**
   * Get the validation property from a {@link Configuration}.
   *
   * @param configuration the configuration to examine
   * @return the value of the validation property
   */
  public static boolean getValidation(Configuration configuration) {
    return configuration.getBoolean(VALIDATION, false);
  }

  /**
   * Get the codec property from a {@link TaskAttemptContext}.
   *
   * @param taskAttemptContext the task attempt context to examine
   * @return the compression codec name from the task attempt context's codec configuration
   */
  private CompressionCodecName getCodec(TaskAttemptContext taskAttemptContext) {
    return CodecConfig.from(taskAttemptContext).getCodec();
  }

  /**
   * Sets the max padding size property in a {@link JobContext}.
   *
   * @param jobContext the context to update
   * @param maxPaddingSize the value to set the property to
   */
  public static void setMaxPaddingSize(JobContext jobContext, int maxPaddingSize) {
    setMaxPaddingSize(getConfiguration(jobContext), maxPaddingSize);
  }

  /**
   * Sets the max padding size property in a {@link Configuration}.
   *
   * @param conf the configuration to update
   * @param maxPaddingSize the value to set the property to
   */
  public static void setMaxPaddingSize(Configuration conf, int maxPaddingSize) {
    conf.setInt(MAX_PADDING_BYTES, maxPaddingSize);
  }

  /**
   * Get the max padding size property from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the value of the max padding size property
   */
  public static int getMaxPaddingSize(Configuration conf) {
    return conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT);
  }

  /**
   * Sets the column index truncate length property in a {@link JobContext}.
   *
   * @param jobContext the context to update
   * @param length the value to set the length to
   */
  public static void setColumnIndexTruncateLength(JobContext jobContext, int length) {
    setColumnIndexTruncateLength(getConfiguration(jobContext), length);
  }

  /**
   * Sets the column index truncate length property in a {@link Configuration}.
   *
   * @param conf the configuration to update
   * @param length the value to set the length to
   */
  public static void setColumnIndexTruncateLength(Configuration conf, int length) {
    conf.setInt(COLUMN_INDEX_TRUNCATE_LENGTH, length);
  }

  /**
   * Get the column index truncate length property from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the value of the column index truncate length property
   */
  public static int getColumnIndexTruncateLength(Configuration conf) {
    return conf.getInt(COLUMN_INDEX_TRUNCATE_LENGTH, ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);
  }

  /**
   * Sets the statistics truncate length property in a {@link JobContext}.
   *
   * @param jobContext the context to update
   * @param length the value to set the length to
   */
  public static void setStatisticsTruncateLength(JobContext jobContext, int length) {
    setStatisticsTruncateLength(getConfiguration(jobContext), length);
  }

  /**
   * Sets the statistics truncate length property in a {@link Configuration}.
   *
   * @param conf the configuration to update
   * @param length the value to set the length to
   */
  private static void setStatisticsTruncateLength(Configuration conf, int length) {
    conf.setInt(STATISTICS_TRUNCATE_LENGTH, length);
  }

  /**
   * Get the statistics truncate length property from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the value of the statistics truncate length property
   */
  public static int getStatisticsTruncateLength(Configuration conf) {
    return conf.getInt(STATISTICS_TRUNCATE_LENGTH, ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH);
  }

  /**
   * Sets the page row count limit property in a {@link JobContext}.
   *
   * @param jobContext the context to update
   * @param rowCount the value to set the property to
   */
  public static void setPageRowCountLimit(JobContext jobContext, int rowCount) {
    setPageRowCountLimit(getConfiguration(jobContext), rowCount);
  }

  /**
   * Sets the page row count limit property in a {@link Configuration}.
   *
   * @param conf the configuration to update
   * @param rowCount the value to set the property to
   */
  public static void setPageRowCountLimit(Configuration conf, int rowCount) {
    conf.setInt(PAGE_ROW_COUNT_LIMIT, rowCount);
  }

  /**
   * Get the page row count limit property from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the value of the page row count limit property
   */
  public static int getPageRowCountLimit(Configuration conf) {
    return conf.getInt(PAGE_ROW_COUNT_LIMIT, ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
  }

  /**
   * Sets the page write checksum enabled property in a {@link JobContext}.
   *
   * @param jobContext the context to update
   * @param val the value to set the property to
   */
  public static void setPageWriteChecksumEnabled(JobContext jobContext, boolean val) {
    setPageWriteChecksumEnabled(getConfiguration(jobContext), val);
  }

  /**
   * Sets the page write checksum enabled property in a {@link Configuration}.
   *
   * @param conf the configuration to update
   * @param val the value to set the property to
   */
  public static void setPageWriteChecksumEnabled(Configuration conf, boolean val) {
    conf.setBoolean(PAGE_WRITE_CHECKSUM_ENABLED, val);
  }

  /**
   * Get the page write checksum enabled property from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @return the value of the page write checksum enabled property
   */
  public static boolean getPageWriteChecksumEnabled(Configuration conf) {
    return conf.getBoolean(PAGE_WRITE_CHECKSUM_ENABLED, ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED);
  }

  private WriteSupport<T> writeSupport;
  private ParquetOutputCommitter committer;

  /**
   * constructor used when this OutputFormat in wrapped in another one (In Pig for example)
   *
   * @param writeSupport the class used to convert the incoming records
   * @param <S> the Java write support type
   */
  public <S extends WriteSupport<T>> ParquetOutputFormat(S writeSupport) {
    this.writeSupport = writeSupport;
  }

  /**
   * used when directly using the output format and configuring the write support implementation
   * using parquet.write.support.class
   *
   * @param <S> the Java write support type
   */
  public <S extends WriteSupport<T>> ParquetOutputFormat() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return getRecordWriter(taskAttemptContext, Mode.CREATE);
  }

  /**
   * Get the record writer from a {@link TaskAttemptContext}.
   *
   * @param taskAttemptContext the task attempt context to examine
   * @param mode the mode of the record writer
   * @return the record writer from the task attempt context
   */
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext, Mode mode)
      throws IOException, InterruptedException {

    final Configuration conf = getConfiguration(taskAttemptContext);

    CompressionCodecName codec = getCodec(taskAttemptContext);
    String extension = codec.getExtension() + ".parquet";
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return getRecordWriter(conf, file, codec, mode);
  }

  /**
   * Get the record writer from a {@link TaskAttemptContext}.
   *
   * @param taskAttemptContext the task attempt context to examine
   * @param file the {@link Path} for the record writer
   * @return the record writer from the task attempt context
   */
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext, Path file)
    throws IOException, InterruptedException {
    return getRecordWriter(taskAttemptContext, file, Mode.CREATE);
  }

  /**
   * Get the record writer from a {@link TaskAttemptContext}.
   *
   * @param taskAttemptContext the task attempt context to examine
   * @param file the {@link Path} for the record writer
   * @param mode the mode of the record writer
   * @return the record writer from the task attempt context
   */
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext, Path file, Mode mode)
      throws IOException, InterruptedException {
    return getRecordWriter(getConfiguration(taskAttemptContext), file, getCodec(taskAttemptContext), mode);
  }

  /**
   * Get the record writer from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @param file the {@link Path} for the record writer
   * @param codec the codec of the record writer
   * @return the record writer from the task attempt context
   */
  public RecordWriter<Void, T> getRecordWriter(Configuration conf, Path file, CompressionCodecName codec)
      throws IOException, InterruptedException {
    return getRecordWriter(conf, file, codec, Mode.CREATE);
  }

  /**
   * Get the record writer from a {@link Configuration}.
   *
   * @param conf the configuration to examine
   * @param file the {@link Path} for the record writer
   * @param codec the codec of the record writer
   * @param mode the mode of the record writer
   * @return the record writer from the task attempt context
   */
  public RecordWriter<Void, T> getRecordWriter(Configuration conf, Path file, CompressionCodecName codec, Mode mode)
        throws IOException, InterruptedException {
    final WriteSupport<T> writeSupport = getWriteSupport(conf);

    ParquetProperties.Builder propsBuilder = ParquetProperties.builder()
        .withPageSize(getPageSize(conf))
        .withDictionaryPageSize(getDictionaryPageSize(conf))
        .withDictionaryEncoding(getEnableDictionary(conf))
        .withWriterVersion(getWriterVersion(conf))
        .estimateRowCountForPageSizeCheck(getEstimatePageSizeCheck(conf))
        .withMinRowCountForPageSizeCheck(getMinRowCountForPageSizeCheck(conf))
        .withMaxRowCountForPageSizeCheck(getMaxRowCountForPageSizeCheck(conf))
        .withPageValueCountThreshold(getValueCountThreshold(conf))
        .withColumnIndexTruncateLength(getColumnIndexTruncateLength(conf))
        .withStatisticsTruncateLength(getStatisticsTruncateLength(conf))
        .withMaxBloomFilterBytes(getBloomFilterMaxBytes(conf))
        .withBloomFilterEnabled(getBloomFilterEnabled(conf))
        .withAdaptiveBloomFilterEnabled(getAdaptiveBloomFilterEnabled(conf))
        .withPageRowCountLimit(getPageRowCountLimit(conf))
        .withPageWriteChecksumEnabled(getPageWriteChecksumEnabled(conf));
    new ColumnConfigParser()
        .withColumnConfig(ENABLE_DICTIONARY, key -> conf.getBoolean(key, false), propsBuilder::withDictionaryEncoding)
        .withColumnConfig(BLOOM_FILTER_ENABLED, key -> conf.getBoolean(key, false),
            propsBuilder::withBloomFilterEnabled)
        .withColumnConfig(BLOOM_FILTER_EXPECTED_NDV, key -> conf.getLong(key, -1L), propsBuilder::withBloomFilterNDV)
        .withColumnConfig(BLOOM_FILTER_FPP, key -> conf.getDouble(key, ParquetProperties.DEFAULT_BLOOM_FILTER_FPP),
            propsBuilder::withBloomFilterFPP)
        .withColumnConfig(
          BLOOM_FILTER_CANDIDATES_NUMBER,
          key -> conf.getInt(key, ParquetProperties.DEFAULT_BLOOM_FILTER_CANDIDATES_NUMBER),
          propsBuilder::withBloomFilterCandidatesNumber)
        .parseConfig(conf);

    ParquetProperties props = propsBuilder.build();

    long blockSize = getLongBlockSize(conf);
    int maxPaddingSize = getMaxPaddingSize(conf);
    boolean validating = getValidation(conf);

    LOG.info(
        "ParquetRecordWriter [block size: {}b, row group padding size: {}b, validating: {}]",
        blockSize, maxPaddingSize, validating);
    LOG.debug("Parquet properties are:\n{}", props);

    WriteContext fileWriteContext = writeSupport.init(conf);

    FileEncryptionProperties encryptionProperties = createEncryptionProperties(conf, file, fileWriteContext);

    ParquetFileWriter w = new ParquetFileWriter(HadoopOutputFile.fromPath(file, conf),
        fileWriteContext.getSchema(), mode, blockSize, maxPaddingSize, props.getColumnIndexTruncateLength(),
        props.getStatisticsTruncateLength(), props.getPageWriteChecksumEnabled(), encryptionProperties);
    w.start();

    float maxLoad = conf.getFloat(ParquetOutputFormat.MEMORY_POOL_RATIO,
        MemoryManager.DEFAULT_MEMORY_POOL_RATIO);
    long minAllocation = conf.getLong(ParquetOutputFormat.MIN_MEMORY_ALLOCATION,
        MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION);
    synchronized (ParquetOutputFormat.class) {
      if (memoryManager == null) {
        memoryManager = new MemoryManager(maxLoad, minAllocation);
      }
    }
    if (memoryManager.getMemoryPoolRatio() != maxLoad) {
      LOG.warn("The configuration " + MEMORY_POOL_RATIO + " has been set. It should not " +
          "be reset by the new value: " + maxLoad);
    }

    return new ParquetRecordWriter<>(
        w,
        writeSupport,
        fileWriteContext.getSchema(),
        fileWriteContext.getExtraMetaData(),
        blockSize,
        codec,
        validating,
        props,
        memoryManager,
        conf);
  }

  /**
   * @param configuration to find the configuration for the write support class
   * @return the configured write support
   */
  @SuppressWarnings("unchecked")
  public WriteSupport<T> getWriteSupport(Configuration configuration){
    if (writeSupport != null) return writeSupport;
    Class<?> writeSupportClass = getWriteSupportClass(configuration);
    try {
      return (WriteSupport<T>) Objects
          .requireNonNull(writeSupportClass, "writeSupportClass cannot be null")
          .newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, e);
    }
  }

  /**
   * Get the {@link OutputCommitter} from a {@link TaskAttemptContext}.
   *
   * @param context the task attempt context to examine
   * @return the output committer from the task attempt context
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new ParquetOutputCommitter(output, context);
    }
    return committer;
  }

  /**
   * This memory manager is for all the real writers (InternalParquetRecordWriter) in one task.
   */
  private static MemoryManager memoryManager;

  /**
   * Get the memory manager.
   *
   * @return the memory manager for all the real writers in one task
   */
  public synchronized static MemoryManager getMemoryManager() {
    return memoryManager;
  }

  /**
   * Create the {@link FileEncryptionProperties} for a file.
   *
   * @param fileHadoopConfig the configuration to create the properties for
   * @param tempFilePath the path of the file to create the properties for
   * @param fileWriteContext the write context of the file to create the properties for
   * @return the file's {@link FileEncryptionProperties}
   */
  public static FileEncryptionProperties createEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
      WriteContext fileWriteContext) {
    EncryptionPropertiesFactory cryptoFactory = EncryptionPropertiesFactory.loadFactory(fileHadoopConfig);
    if (null == cryptoFactory) {
      return null;
    }
    return cryptoFactory.getFileEncryptionProperties(fileHadoopConfig, tempFilePath, fileWriteContext);
  }
}
