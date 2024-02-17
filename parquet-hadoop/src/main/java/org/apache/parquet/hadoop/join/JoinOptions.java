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
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import java.util.List;
import java.util.stream.Collectors;

public class JoinOptions {

  private final ParquetConfiguration conf;
  private final List<InputFile> inputFilesL;
  private final List<List<InputFile>> inputFilesR;
  private final OutputFile outputFile;
  private final IndexCache.CacheStrategy indexCacheStrategy;

  private JoinOptions(
      ParquetConfiguration conf,
      List<InputFile> inputFilesL,
      List<List<InputFile>> inputFilesR,
      OutputFile outputFile,
      IndexCache.CacheStrategy indexCacheStrategy) {
    this.conf = conf;
    this.inputFilesL = inputFilesL;
    this.inputFilesR = inputFilesR;
    this.outputFile = outputFile;
    this.indexCacheStrategy = indexCacheStrategy;
  }

  public Configuration getConf() {
    return ConfigurationUtil.createHadoopConfiguration(conf);
  }

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

  public List<InputFile> getParquetInputFilesL() {
    return inputFilesL;
  }

  public List<List<InputFile>> getParquetInputFilesR() {
    return inputFilesR;
  }

  public Path getOutputFile() {
    if (outputFile instanceof HadoopOutputFile) {
      HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) outputFile;
      return new Path(hadoopOutputFile.getPath());
    } else {
      throw new RuntimeException("The output file does not have an associated Hadoop Path.");
    }
  }

  public OutputFile getParquetOutputFile() {
    return outputFile;
  }

  public IndexCache.CacheStrategy getIndexCacheStrategy() {
    return indexCacheStrategy;
  }

  public static class Builder {
    private final ParquetConfiguration conf;
    private final List<InputFile> inputFilesL;
    private final List<List<InputFile>> inputFilesR;
    private final OutputFile outputFile;
    private IndexCache.CacheStrategy indexCacheStrategy = IndexCache.CacheStrategy.NONE;

    public Builder(Configuration conf, List<Path> inputFilesL, List<List<Path>> inputFilesR, Path outputFile) {
      this.conf = new HadoopParquetConfiguration(conf);
      this.inputFilesL = inputFilesL
          .stream()
          .map(x -> HadoopInputFile.fromPathUnchecked(x, conf))
          .collect(Collectors.toList());
      this.inputFilesR = inputFilesR
          .stream()
          .map(x -> x.stream().map(y -> (InputFile) HadoopInputFile.fromPathUnchecked(y, conf)).collect(Collectors.toList()))
          .collect(Collectors.toList());
      this.outputFile = HadoopOutputFile.fromPathUnchecked(outputFile, conf);
    }

    public Builder(
        ParquetConfiguration conf,
        List<InputFile> inputFilesL,
        List<List<InputFile>> inputFilesR,
        OutputFile outputFile) {
      this.conf = conf;
      this.inputFilesL = inputFilesL;
      this.inputFilesR = inputFilesR;
      this.outputFile = outputFile;
    }

    public Builder indexCacheStrategy(IndexCache.CacheStrategy cacheStrategy) {
      this.indexCacheStrategy = cacheStrategy;
      return this;
    }

    public JoinOptions build() {
      // TODO move these check to the ParquetJoiner itself?
      Preconditions.checkArgument(inputFilesL != null && !inputFilesL.isEmpty(), "Input fileL can't be NULL or empty");
      Preconditions.checkArgument(inputFilesR != null && !inputFilesR.isEmpty(), "Input fileR can't be NULL or empty");
      Preconditions.checkArgument(inputFilesR.stream().allMatch(x -> x != null && !x.isEmpty()), "Input fileR elements can't be NULL or empty");
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
