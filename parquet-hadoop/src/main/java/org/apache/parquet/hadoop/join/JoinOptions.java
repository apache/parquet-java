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
package org.apache.parquet.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.rewrite.MaskMode;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A set of options to create a ParquetJoiner.
 */
public class JoinOptions {

  private final ParquetConfiguration conf;
  private final List<InputFile> inputFilesL;
  private final List<InputFile> inputFilesR;
  private final OutputFile outputFile;
  private final IndexCache.CacheStrategy indexCacheStrategy;

  private JoinOptions(
      ParquetConfiguration conf,
      List<InputFile> inputFilesL,
      List<InputFile> inputFilesR,
      OutputFile outputFile,
      IndexCache.CacheStrategy indexCacheStrategy) {
    this.conf = conf;
    this.inputFilesL = inputFilesL;
    this.inputFilesR = inputFilesR;
    this.outputFile = outputFile;
    this.indexCacheStrategy = indexCacheStrategy;
  }

  /**
   * Gets the {@link Configuration} part of the join options.
   *
   * @return the associated {@link Configuration}
   */
  public Configuration getConf() {
    return ConfigurationUtil.createHadoopConfiguration(conf);
  }

  /**
   * Gets the {@link ParquetConfiguration} part of the join options.
   *
   * @return the associated {@link ParquetConfiguration}
   */
  public ParquetConfiguration getParquetConfiguration() {
    return conf;
  }

  private List<Path> getInputFiles(List<InputFile> in) {
    return in.stream()
        .map(f -> {
          if (f instanceof HadoopOutputFile) {
            HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) f;
            return new Path(hadoopOutputFile.getPath());
          } else {
            throw new RuntimeException("The input files do not all have an associated Hadoop Path.");
          }
        })
        .collect(Collectors.toList());
  }

  /**
   * Gets the input {@link Path}s for the join if they exist for all input files,
   * otherwise throws a {@link RuntimeException}.
   *
   * @return a {@link List} of the associated input {@link Path}s
   */
  public List<Path> getInputFilesL() {
    return getInputFiles(inputFilesL);
  }

  public List<Path> getInputFilesR() {
    return getInputFiles(inputFilesR);
  }

  /**
   * Gets the {@link InputFile}s for the join.
   *
   * @return a {@link List} of the associated {@link InputFile}s
   */
  public List<InputFile> getParquetInputFilesL() {
    return inputFilesL;
  }

  public List<InputFile> getParquetInputFilesR() {
    return inputFilesR;
  }

  /**
   * Get the {@link Path} for the join if it exists, otherwise throws a {@link RuntimeException}.
   *
   * @return the associated {@link Path} if it exists
   */
  public Path getOutputFile() {
    if (outputFile instanceof HadoopOutputFile) {
      HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) outputFile;
      return new Path(hadoopOutputFile.getPath());
    } else {
      throw new RuntimeException("The output file does not have an associated Hadoop Path.");
    }
  }

  /**
   * Get the {@link OutputFile} for the join.
   *
   * @return the associated {@link OutputFile}
   */
  public OutputFile getParquetOutputFile() {
    return outputFile;
  }

  public IndexCache.CacheStrategy getIndexCacheStrategy() {
    return indexCacheStrategy;
  }

  // Builder to create a JoinerOptions.
  public static class Builder {
    private final ParquetConfiguration conf;
    private final List<InputFile> inputFilesL;
    private final List<InputFile> inputFilesR;
    private final OutputFile outputFile;
    private IndexCache.CacheStrategy indexCacheStrategy = IndexCache.CacheStrategy.NONE;

    /**
     * Create a builder to create a RewriterOptions.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFile  input file path to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(Configuration conf, Path inputFileL, Path inputFileR, Path outputFile) {
      this(
          new HadoopParquetConfiguration(conf),
          HadoopInputFile.fromPathUnchecked(inputFileL, conf),
          HadoopInputFile.fromPathUnchecked(inputFileR, conf),
          HadoopOutputFile.fromPathUnchecked(outputFile, conf));
    }

    /**
     * Create a builder to create a RewriterOptions.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFile  input file to read from
     * @param outputFile output file to rewrite to
     */
    public Builder(ParquetConfiguration conf, InputFile inputFileL, InputFile inputFileR, OutputFile outputFile) {
      this(conf, Collections.singletonList(inputFileL), Collections.singletonList(inputFileR), outputFile);
    }

    /**
     * Create a builder to create a RewriterOptions.
     * <p>
     * Please note that if merging more than one file, the schema of all files must be the same.
     * Otherwise, the rewrite will fail.
     * <p>
     * The rewrite will keep original row groups from all input files. This may not be optimal
     * if row groups are very small and will not solve small file problems. Instead, it will
     * make it worse to have a large file footer in the output file.
     * TODO: support rewrite by record to break the original row groups into reasonable ones.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFilesL list of input file paths to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(Configuration conf, List<Path> inputFilesL, List<Path> inputFilesR, Path outputFile) {
      this.conf = new HadoopParquetConfiguration(conf);
      this.inputFilesL = new ArrayList<>(inputFilesL.size());
      for (Path inputFile : inputFilesL) {
        this.inputFilesL.add(HadoopInputFile.fromPathUnchecked(inputFile, conf));
      }
      this.inputFilesR = new ArrayList<>(inputFilesR.size());
      for (Path inputFile : inputFilesR) {
        this.inputFilesR.add(HadoopInputFile.fromPathUnchecked(inputFile, conf));
      }
      this.outputFile = HadoopOutputFile.fromPathUnchecked(outputFile, conf);
    }

    /**
     * Create a builder to create a RewriterOptions.
     * <p>
     * Please note that if merging more than one file, the schema of all files must be the same.
     * Otherwise, the rewrite will fail.
     * <p>
     * The rewrite will keep original row groups from all input files. This may not be optimal
     * if row groups are very small and will not solve small file problems. Instead, it will
     * make it worse to have a large file footer in the output file.
     * TODO: support rewrite by record to break the original row groups into reasonable ones.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFilesL list of input file paths to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(
        ParquetConfiguration conf,
        List<InputFile> inputFilesL,
        List<InputFile> inputFilesR,
        OutputFile outputFile) {
      this.conf = conf;
      this.inputFilesL = inputFilesL;
      this.inputFilesR = inputFilesR;
      this.outputFile = outputFile;
    }

    /**
     * Add an input file to read from.
     *
     * @param path input file path to read from
     * @return self
     */
    public Builder addInputFileL(Path path) {
      this.inputFilesL.add(
          HadoopInputFile.fromPathUnchecked(path, ConfigurationUtil.createHadoopConfiguration(conf)));
      return this;
    }

    public Builder addInputFileR(Path path) {
      this.inputFilesR.add(
          HadoopInputFile.fromPathUnchecked(path, ConfigurationUtil.createHadoopConfiguration(conf)));
      return this;
    }

    /**
     * Add an input file to read from.
     *
     * @param inputFile input file to read from
     * @return self
     */
    public Builder addInputFileL(InputFile inputFile) {
      this.inputFilesL.add(inputFile);
      return this;
    }

    public Builder addInputFileR(InputFile inputFile) {
      this.inputFilesR.add(inputFile);
      return this;
    }

    /**
     * Set the index(ColumnIndex, Offset and BloomFilter) cache strategy.
     * <p>
     * This could reduce the random seek while rewriting with PREFETCH_BLOCK strategy, NONE by default.
     *
     * @param cacheStrategy the index cache strategy, supports: {@link IndexCache.CacheStrategy#NONE} or
     *                      {@link IndexCache.CacheStrategy#PREFETCH_BLOCK}
     * @return self
     */
    public Builder indexCacheStrategy(IndexCache.CacheStrategy cacheStrategy) {
      this.indexCacheStrategy = cacheStrategy;
      return this;
    }

    /**
     * Build the RewriterOptions.
     *
     * @return a RewriterOptions
     */
    public JoinOptions build() {
      Preconditions.checkArgument(inputFilesL != null && !inputFilesL.isEmpty(), "Input file is required");
      Preconditions.checkArgument(inputFilesR != null && !inputFilesR.isEmpty(), "Input file is required");
      Preconditions.checkArgument(outputFile != null, "Output file is required");

      return new JoinOptions(
          conf,
          inputFilesL,
          inputFilesR,
          outputFile,
          indexCacheStrategy);
    }
  }
}
