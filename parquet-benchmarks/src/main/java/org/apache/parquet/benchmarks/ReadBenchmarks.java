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
package org.apache.parquet.benchmarks;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadBenchmarks {
  private void read(Path parquetFile, int nRows, Blackhole blackhole) throws IOException
  {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFile).withConf(configuration).build();
    for (int i = 0; i < nRows; i++) {
      Group group = reader.read();
      blackhole.consume(group.getBinary("binary_field", 0));
      blackhole.consume(group.getInteger("int32_field", 0));
      blackhole.consume(group.getLong("int64_field", 0));
      blackhole.consume(group.getBoolean("boolean_field", 0));
      blackhole.consume(group.getFloat("float_field", 0));
      blackhole.consume(group.getDouble("double_field", 0));
      blackhole.consume(group.getBinary("flba_field", 0));
      blackhole.consume(group.getInt96("int96_field", 0));
    }
    reader.close();
  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS256MPS4MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS256M_PS4M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS256MPS8MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS256M_PS8M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS512MPS4MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS512M_PS4M, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsBS512MPS8MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS512M_PS8M, ONE_MILLION, blackhole);
  }

  //TODO how to handle lzo jar?
//  @Benchmark
//  public void read1MRowsDefaultBlockAndPageSizeLZO(Blackhole blackhole)
//          throws IOException
//  {
//    read(parquetFile_1M_LZO, ONE_MILLION, blackhole);
//  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeSNAPPY(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_SNAPPY, ONE_MILLION, blackhole);
  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeGZIP(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_GZIP, ONE_MILLION, blackhole);
  }

  private void concurrentRead(Path f, int numThreads, Blackhole blackhole) throws IOException, InterruptedException
  {
    List<Thread> threads = new ArrayList<>();
    // Read the file metadata for block assignments afterwards.
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, f, NO_FILTER);
    List<BlockMetaData> blocks = footer.getBlocks();
    int totalRowBlocks = blocks.size();
    if (totalRowBlocks < numThreads) {
      numThreads = totalRowBlocks;
    }

    // Assign blocks to each thread
    int index = 0;
    int slop = totalRowBlocks % numThreads;
    for (int i = 0; i < numThreads; i++) {
      int numRows = (totalRowBlocks - slop) / numThreads;
      // Try load balancing
      if (i < slop) {
        ++numRows;
      }
      List<BlockMetaData> subBlocks = blocks.subList(index, index + numRows);
      FileMetaData meta = footer.getFileMetaData();
      Thread t = new ParquetScanner(f, meta, subBlocks, meta.getSchema().getColumns(), blackhole);
      t.start();
      threads.add(t);
      index += numRows;
    }

    for (Thread t: threads) {
      t.join();
    }
  }

  @Benchmark
  @Threads(1)
  public void readUncompressed16Thread(Blackhole blackhole)
    throws IOException, InterruptedException
  {
    concurrentRead(file_1M_BS64K_PS4K, SIXTEEN_THREADS, blackhole);
  }

  @Benchmark
  @Threads(1)
  public void readSNAPPY16Thread(Blackhole blackhole)
    throws IOException, InterruptedException
  {
    concurrentRead(file_1M_BS64K_PS4K_SNAPPY, SIXTEEN_THREADS, blackhole);
  }

  @Benchmark
  @Threads(1)
  public void readGZIP16Thread(Blackhole blackhole)
    throws IOException, InterruptedException
  {
    concurrentRead(file_1M_BS64K_PS4K_GZIP, SIXTEEN_THREADS, blackhole);
  }
}
