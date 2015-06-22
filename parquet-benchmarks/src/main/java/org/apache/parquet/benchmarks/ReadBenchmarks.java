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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

import java.io.IOException;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
public class ReadBenchmarks {
  private Configuration configuration;
  private GroupReadSupport groupReadSupport;

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    configuration = new Configuration();
    groupReadSupport = new GroupReadSupport();
  }

  private void read(Path parquetFile, Blackhole blackhole) throws IOException
  {
    ParquetReader<Group> reader = ParquetReader.builder(groupReadSupport, parquetFile).withConf(configuration).build();

    Group group;
    while ((group = reader.read()) != null) {
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
    read(file_1M, blackhole);
  }

  @Benchmark
  public void read1MRowsBS256MPS4MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS256M_PS4M, blackhole);
  }

  @Benchmark
  public void read1MRowsBS256MPS8MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS256M_PS8M, blackhole);
  }

  @Benchmark
  public void read1MRowsBS512MPS4MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS512M_PS4M, blackhole);
  }

  @Benchmark
  public void read1MRowsBS512MPS8MUncompressed(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_BS512M_PS8M, blackhole);
  }

  //TODO how to handle lzo jar?
//  @Benchmark
//  public void read1MRowsDefaultBlockAndPageSizeLZO(Blackhole blackhole)
//          throws IOException
//  {
//    read(parquetFile_1M_LZO, blackhole);
//  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeSNAPPY(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_SNAPPY, blackhole);
  }

  @Benchmark
  public void read1MRowsDefaultBlockAndPageSizeGZIP(Blackhole blackhole)
          throws IOException
  {
    read(file_1M_GZIP, blackhole);
  }
}
