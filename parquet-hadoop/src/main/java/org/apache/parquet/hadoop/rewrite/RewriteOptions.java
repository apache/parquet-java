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
package org.apache.parquet.hadoop.rewrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

// A set of options to create a ParquetRewriter.
public class RewriteOptions {

  final Configuration conf;
  final List<Path> inputFiles;
  final Path outputFile;
  final List<String> pruneColumns;
  final CompressionCodecName newCodecName;
  final Map<String, MaskMode> maskColumns;
  final List<String> encryptColumns;
  final FileEncryptionProperties fileEncryptionProperties;

  private RewriteOptions(Configuration conf,
                         List<Path> inputFiles,
                         Path outputFile,
                         List<String> pruneColumns,
                         CompressionCodecName newCodecName,
                         Map<String, MaskMode> maskColumns,
                         List<String> encryptColumns,
                         FileEncryptionProperties fileEncryptionProperties) {
    this.conf = conf;
    this.inputFiles = inputFiles;
    this.outputFile = outputFile;
    this.pruneColumns = pruneColumns;
    this.newCodecName = newCodecName;
    this.maskColumns = maskColumns;
    this.encryptColumns = encryptColumns;
    this.fileEncryptionProperties = fileEncryptionProperties;
  }

  public Configuration getConf() {
    return conf;
  }

  public List<Path> getInputFiles() {
    return inputFiles;
  }

  public Path getOutputFile() {
    return outputFile;
  }

  public List<String> getPruneColumns() {
    return pruneColumns;
  }

  public CompressionCodecName getNewCodecName() {
    return newCodecName;
  }

  public Map<String, MaskMode> getMaskColumns() {
    return maskColumns;
  }

  public List<String> getEncryptColumns() {
    return encryptColumns;
  }

  public FileEncryptionProperties getFileEncryptionProperties() {
    return fileEncryptionProperties;
  }

  // Builder to create a RewriterOptions.
  public static class Builder {
    private Configuration conf;
    private List<Path> inputFiles;
    private Path outputFile;
    private List<String> pruneColumns;
    private CompressionCodecName newCodecName;
    private Map<String, MaskMode> maskColumns;
    private List<String> encryptColumns;
    private FileEncryptionProperties fileEncryptionProperties;

    public Builder(Configuration conf, Path inputFile, Path outputFile) {
      this.conf = conf;
      this.inputFiles = Arrays.asList(inputFile);
      this.outputFile = outputFile;
    }

    public Builder(Configuration conf, List<Path> inputFiles, Path outputFile) {
      this.conf = conf;
      this.inputFiles = inputFiles;
      this.outputFile = outputFile;
    }

    public Builder prune(List<String> columns) {
      this.pruneColumns = columns;
      return this;
    }

    public Builder transform(CompressionCodecName newCodecName) {
      this.newCodecName = newCodecName;
      return this;
    }

    public Builder mask(Map<String, MaskMode> maskColumns) {
      this.maskColumns = maskColumns;
      return this;
    }

    public Builder encrypt(List<String> encryptColumns) {
      this.encryptColumns = encryptColumns;
      return this;
    }

    public Builder encryptionProperties(FileEncryptionProperties fileEncryptionProperties) {
      this.fileEncryptionProperties = fileEncryptionProperties;
      return this;
    }

    public Builder addInputFile(Path path) {
      this.inputFiles.add(path);
      return this;
    }

    public RewriteOptions build() {
      Preconditions.checkArgument(inputFiles != null && !inputFiles.isEmpty(), "Input file is required");
      Preconditions.checkArgument(outputFile != null, "Output file is required");

      if (pruneColumns != null) {
        if (maskColumns != null) {
          for (String pruneColumn : pruneColumns) {
            Preconditions.checkArgument(!maskColumns.containsKey(pruneColumn),
                    "Cannot prune and mask same column");
          }
        }

        if (encryptColumns != null) {
          for (String pruneColumn : pruneColumns) {
            Preconditions.checkArgument(!encryptColumns.contains(pruneColumn),
                    "Cannot prune and encrypt same column");
          }
        }
      }

      if (encryptColumns != null && !encryptColumns.isEmpty()) {
        Preconditions.checkArgument(fileEncryptionProperties != null,
                "FileEncryptionProperties is required when encrypting columns");
      }

      if (fileEncryptionProperties != null) {
        Preconditions.checkArgument(encryptColumns != null && !encryptColumns.isEmpty(),
                "Encrypt columns is required when FileEncryptionProperties is set");
      }

      return new RewriteOptions(conf,
              inputFiles,
              outputFile,
              pruneColumns,
              newCodecName,
              maskColumns,
              encryptColumns,
              fileEncryptionProperties);
    }
  }

}
