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
package org.apache.parquet.crypto;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.EncDecProperties;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

/**
 * Verifies that the signature of an encrypted file with a plaintext (signed) footer is checked
 * correctly: a valid signature is accepted and a tampered GCM tag is rejected with a {@link
 * TagVerificationException}. This exercises {@code ParquetMetadataConverter.verifyFooterIntegrity}.
 */
public class TestFooterIntegrity {

  private static final MessageType SCHEMA = new MessageType("schema", new PrimitiveType(OPTIONAL, INT64, "id"));

  private String writeSignedPlaintextFooterFile() throws IOException {
    Configuration conf = new Configuration();
    return new TestFileBuilder(conf, SCHEMA)
        .withNumRecord(100)
        .withEncryptColumns(new String[] {"id"})
        .build()
        .getFileName();
  }

  private static ParquetMetadata readFooter(String file) throws IOException {
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
        .withDecryption(EncDecProperties.getFileDecryptionProperties())
        .build();
    InputFile inputFile = HadoopInputFile.fromPath(new Path(file), new Configuration());
    try (SeekableInputStream in = inputFile.newStream()) {
      return ParquetFileReader.readFooter(inputFile, readOptions, in);
    }
  }

  @Test
  public void validSignatureIsAccepted() throws IOException {
    ParquetMetadata metaData = readFooter(writeSignedPlaintextFooterFile());
    assertFalse(metaData.getBlocks().isEmpty());
  }

  @Test
  public void tamperedFooterSignatureIsRejected() throws IOException {
    String file = writeSignedPlaintextFooterFile();

    // Trailer layout of a signed plaintext footer:
    //   [serialized footer][nonce (12)][GCM tag (16)][footer length (4)][MAGIC (4)]
    // Flip a byte inside the GCM tag so the recomputed tag no longer matches.
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      long tagBytePosition =
          raf.length() - ParquetFileWriter.MAGIC.length - Integer.BYTES - AesCipher.GCM_TAG_LENGTH;
      raf.seek(tagBytePosition);
      int original = raf.read();
      raf.seek(tagBytePosition);
      raf.write(original ^ 0x01);
    }
    // Drop the local filesystem CRC side-file so the tampered bytes reach the Parquet reader.
    File source = new File(file);
    Files.deleteIfExists(new File(source.getParent(), "." + source.getName() + ".crc").toPath());

    assertThrows(TagVerificationException.class, () -> readFooter(file));
  }
}
