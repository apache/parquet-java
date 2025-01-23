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

import static org.openjdk.jmh.annotations.Scope.Thread;

import com.google.protobuf.Message;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Thread)
public class ProtoWriteBenchmarks extends WriteBenchmarks {
  @Param({"OFF", "REQUIRED_ALL"})
  public ProtoWriteSupport.CodegenMode codegenMode;

  @Param({"Test30Int32", "Test100Int32", "Test30String", "Test1"})
  public String protoClass;

  @Setup(Level.Iteration)
  public void setup() {
    Class<Message> messageClass;
    try {
      messageClass = (Class<Message>) Class.forName("org.apache.parquet.benchmarks.Messages$" + protoClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    dataGenerator = new ProtoDataGenerator<>(messageClass, codegenMode);
    // clean existing test data at the beginning of each iteration
    dataGenerator.cleanup();
  }
}
