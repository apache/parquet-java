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
package org.apache.parquet.cli.util;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;

public class RawUtils {

  private static final ObjectMapper MAPPER = createObjectMapper();

  public static FileMetaData readFooter(SeekableInputStream inputStream, long fileLen) throws IOException {
    // Read footer length and magic string - with a single seek
    byte[] magic = new byte[MAGIC.length];
    long fileMetadataLengthIndex = fileLen - magic.length - 4;
    inputStream.seek(fileMetadataLengthIndex);
    int fileMetadataLength = readIntLittleEndian(inputStream);
    inputStream.readFully(magic);

    if (Arrays.equals(EFMAGIC, magic)) {
      throw new RuntimeException("Parquet files with encrypted footers are not supported.");
    } else if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException(
          "Not a Parquet file (expected magic number at tail, but found " + Arrays.toString(magic) + ')');
    }

    long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException(
          "Corrupted file: the footer index inputStream not within the file: " + fileMetadataIndex);
    }
    inputStream.seek(fileMetadataIndex);

    ByteBuffer footerBytesBuffer = ByteBuffer.allocate(fileMetadataLength);
    inputStream.readFully(footerBytesBuffer);
    footerBytesBuffer.flip();
    InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);
    return Util.readFileMetaData(footerBytesStream);
  }

  public static String prettifyJson(String json) throws JsonProcessingException {
    ObjectMapper mapper = MAPPER;
    Object obj = mapper.readValue(json, Object.class);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
  }

  public static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new ParquetModule());
    return mapper;
  }

  static class ParquetModule extends SimpleModule {

    @Override
    public void setupModule(SetupContext context) {
      super.setupModule(context);
      SimpleSerializers sers = new SimpleSerializers();
      sers.addSerializer(LogicalTypeAnnotation.class, new LogicalTypeAnnotationJsonSerializer());
      context.addSerializers(sers);
    }
  }

  static class LogicalTypeAnnotationJsonSerializer extends JsonSerializer<LogicalTypeAnnotation> {

    @Override
    public void serialize(
        LogicalTypeAnnotation value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeString(value.toString());
    }
  }
}
