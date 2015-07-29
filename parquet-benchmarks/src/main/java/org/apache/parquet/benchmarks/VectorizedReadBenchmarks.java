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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.ColumnVector;
import org.apache.parquet.io.vector.ObjectColumnVector;
import org.apache.parquet.io.vector.RowBatch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

import static org.apache.parquet.benchmarks.BenchmarkFiles.defaultConfiguration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.flbaReadConfiguration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.readAllPrimitivesConfiguration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.readFourPrimitivesConfiguration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.readOnePrimitiveConfiguration;
import static org.apache.parquet.hadoop.ParquetReader.builder;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

public class VectorizedReadBenchmarks {

  @State(Benchmark)
  public static class ReadAllObjects {
    private GroupReadSupport groupReadSupport;
    private ParquetReader<Group> defaultReader;

    @Setup(Invocation)
    public void setup() throws IOException {
      groupReadSupport = new GroupReadSupport();
      defaultReader = builder(groupReadSupport, file_10M_GZIP).withConf(defaultConfiguration).build();
    }

    @TearDown(Invocation)
    public void teardown() throws IOException {
      defaultReader.close();
    }

    @Benchmark
    public void readAllObjects(Blackhole blackhole) throws IOException
    {
      Group group;
      while ((group = defaultReader.read()) != null) {
        blackhole.consume(group.getBinary("binary_field", 0));
        blackhole.consume(group.getInteger("int32_field", 0));
        blackhole.consume(group.getLong("int64_field", 0));
        blackhole.consume(group.getBoolean("boolean_field", 0));
        blackhole.consume(group.getFloat("float_field", 0));
        blackhole.consume(group.getDouble("double_field", 0));
        blackhole.consume(group.getBinary("flba_field", 0));
        blackhole.consume(group.getInt96("int96_field", 0));
      }
    }

    @Benchmark
    public void vectorReadAllObjects(Blackhole blackhole) throws IOException
    {
      for (RowBatch batch = defaultReader.nextBatch(null, Group.class);
           batch != null;
           batch = defaultReader.nextBatch(batch, Group.class)) {
        ObjectColumnVector<Group> objectColumnVector = (ObjectColumnVector) batch.getColumns()[0];
        for (int i = 0 ; i < objectColumnVector.size(); i++) {
          Group group = objectColumnVector.values[i];
          blackhole.consume(group.getBinary("binary_field", 0));
          blackhole.consume(group.getInteger("int32_field", 0));
          blackhole.consume(group.getLong("int64_field", 0));
          blackhole.consume(group.getBoolean("boolean_field", 0));
          blackhole.consume(group.getFloat("float_field", 0));
          blackhole.consume(group.getDouble("double_field", 0));
          blackhole.consume(group.getBinary("flba_field", 0));
          blackhole.consume(group.getInt96("int96_field", 0));
        }
      }
    }
  }

  @State(Benchmark)
  public static class ReadAllPrimitives {
    private GroupReadSupport groupReadSupport;
    private ParquetReader<Group> allPrimitivesReader;

    @Setup(Invocation)
    public void setup() throws IOException {
      groupReadSupport = new GroupReadSupport();
      allPrimitivesReader = builder(groupReadSupport, file_10M_GZIP).withConf(readAllPrimitivesConfiguration).build();
    }

    @TearDown(Invocation)
    public void teardown() throws IOException {
      allPrimitivesReader.close();
    }

    @Benchmark
    public void readAllPrimitives(Blackhole blackhole) throws IOException
    {
      Group group;
      while ((group = allPrimitivesReader.read()) != null) {
        blackhole.consume(group.getInteger("int32_field", 0));
        blackhole.consume(group.getLong("int64_field", 0));
        blackhole.consume(group.getBoolean("boolean_field", 0));
        blackhole.consume(group.getFloat("float_field", 0));
        blackhole.consume(group.getDouble("double_field", 0));
        blackhole.consume(group.getBinary("flba_field", 0));
        blackhole.consume(group.getInt96("int96_field", 0));
      }
    }

    @Benchmark
    public void vectorReadAllPrimitives(Blackhole blackhole) throws IOException
    {
      for (RowBatch batch = allPrimitivesReader.nextBatch(null);
           batch != null;
           batch = allPrimitivesReader.nextBatch(batch)) {
        ColumnVector[] columns = batch.getColumns();
        blackhole.consume(columns[0]);
        blackhole.consume(columns[1]);
        blackhole.consume(columns[2]);
        blackhole.consume(columns[3]);
        blackhole.consume(columns[4]);
        blackhole.consume(columns[5]);
        blackhole.consume(columns[6]);
      }
    }
  }

  @State(Benchmark)
  public static class ReadOnePrimitive {
    private GroupReadSupport groupReadSupport;
    private ParquetReader<Group> onePrimitiveReader;

    @Setup(Invocation)
    public void setup() throws IOException {
      groupReadSupport = new GroupReadSupport();
      onePrimitiveReader = builder(groupReadSupport, file_10M_GZIP).withConf(readOnePrimitiveConfiguration).build();
    }

    @TearDown(Invocation)
    public void teardown() throws IOException {
      onePrimitiveReader.close();
    }

    @Benchmark
    public void readOnePrimitive(Blackhole blackhole) throws IOException
    {
      Group group;
      while ((group = onePrimitiveReader.read()) != null) {
        blackhole.consume(group.getInteger("int32_field", 0));
      }
    }

    @Benchmark
    public void vectorReadOnePrimitive(Blackhole blackhole) throws IOException
    {
      for (RowBatch batch = onePrimitiveReader.nextBatch(null);
           batch != null;
           batch = onePrimitiveReader.nextBatch(batch)) {
        ColumnVector[] columns = batch.getColumns();
        blackhole.consume(columns[0]);
      }
    }
  }

  @State(Benchmark)
  public static class ReadFourPrimitives {
    private GroupReadSupport groupReadSupport;
    private ParquetReader<Group> fourPrimitivesReader;

    @Setup(Invocation)
    public void setup() throws IOException {
      groupReadSupport = new GroupReadSupport();
      fourPrimitivesReader = builder(groupReadSupport, file_10M_GZIP).withConf(readFourPrimitivesConfiguration).build();
    }

    @TearDown(Invocation)
    public void teardown() throws IOException {
      fourPrimitivesReader.close();
    }

    @Benchmark
    public void readFourPrimitives(Blackhole blackhole) throws IOException
    {
      Group group;
      while ((group = fourPrimitivesReader.read()) != null) {
        blackhole.consume(group.getInteger("int32_field", 0));
        blackhole.consume(group.getLong("int64_field", 0));
        blackhole.consume(group.getBoolean("boolean_field", 0));
        blackhole.consume(group.getFloat("float_field", 0));
      }
    }

    @Benchmark
    public void vectorReadFourPrimitives(Blackhole blackhole) throws IOException
    {
      for (RowBatch batch = fourPrimitivesReader.nextBatch(null);
           batch != null;
           batch = fourPrimitivesReader.nextBatch(batch)) {
        ColumnVector[] columns = batch.getColumns();
        blackhole.consume(columns[0]);
        blackhole.consume(columns[1]);
        blackhole.consume(columns[2]);
        blackhole.consume(columns[3]);
      }
    }
  }

  @State(Benchmark)
  public static class ReadFixedLenByteArray {
    private GroupReadSupport groupReadSupport;
    private ParquetReader<Group> flbaReader;

    @Setup(Invocation)
    public void setup() throws IOException {
      groupReadSupport = new GroupReadSupport();
      flbaReader = builder(groupReadSupport, file_10M_GZIP).withConf(flbaReadConfiguration).build();
    }

    @TearDown(Invocation)
    public void teardown() throws IOException {
      flbaReader.close();
    }

    @Benchmark
    public void readFixedLenByteArray(Blackhole blackhole) throws IOException
    {
      Group group;
      while ((group = flbaReader.read()) != null) {
        blackhole.consume(group.getBinary("flba_field", 0));
      }
    }

    @Benchmark
    public void vectorReadFixedLenByteArray(Blackhole blackhole) throws IOException
    {
      for (RowBatch batch = flbaReader.nextBatch(null);
           batch != null;
           batch = flbaReader.nextBatch(batch)) {
        ColumnVector[] columns = batch.getColumns();
        blackhole.consume(columns[0]);
      }
    }
  }
}
