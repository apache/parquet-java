/**
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
package org.apache.parquet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.ParquetConfiguration;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class DirectWriterTest {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  protected interface DirectWriter {
    public void write(RecordConsumer consumer);
  }

  protected Path writeDirect(String type, DirectWriter writer) throws IOException {
    return writeDirect(MessageTypeParser.parseMessageType(type), writer);
  }

  protected Path writeDirect(String type, DirectWriter writer,
                           Map<String, String> metadata) throws IOException {
    return writeDirect(MessageTypeParser.parseMessageType(type), writer, metadata);
  }

  protected Path writeDirect(MessageType type, DirectWriter writer) throws IOException {
    return writeDirect(type, writer, new HashMap<String, String>());
  }

  protected Path writeDirect(MessageType type, DirectWriter writer,
                           Map<String, String> metadata) throws IOException {
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ParquetWriter<Void> parquetWriter = new ParquetWriter<Void>(
        path, new DirectWriteSupport(type, writer, metadata));
    parquetWriter.write(null);
    parquetWriter.close();

    return path;
  }

  protected static class DirectWriteSupport extends WriteSupport<Void> {
    private RecordConsumer recordConsumer;
    private final MessageType type;
    private final DirectWriter writer;
    private final Map<String, String> metadata;

    protected DirectWriteSupport(MessageType type, DirectWriter writer,
                                 Map<String, String> metadata) {
      this.type = type;
      this.writer = writer;
      this.metadata = metadata;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return init((ParquetConfiguration) null);
    }

    @Override
    public WriteContext init(ParquetConfiguration configuration) {
      return new WriteContext(type, metadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Void record) {
      writer.write(recordConsumer);
    }
  }
}
