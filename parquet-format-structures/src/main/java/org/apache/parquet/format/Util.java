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

package org.apache.parquet.format;

import static org.apache.parquet.format.FileMetaData._Fields.CREATED_BY;
import static org.apache.parquet.format.FileMetaData._Fields.ENCRYPTION_ALGORITHM;
import static org.apache.parquet.format.FileMetaData._Fields.FOOTER_SIGNING_KEY_METADATA;
import static org.apache.parquet.format.FileMetaData._Fields.KEY_VALUE_METADATA;
import static org.apache.parquet.format.FileMetaData._Fields.NUM_ROWS;
import static org.apache.parquet.format.FileMetaData._Fields.ROW_GROUPS;
import static org.apache.parquet.format.FileMetaData._Fields.SCHEMA;
import static org.apache.parquet.format.FileMetaData._Fields.VERSION;
import static org.apache.parquet.format.MetadataValidator.validate;
import static org.apache.parquet.format.event.Consumers.fieldConsumer;
import static org.apache.parquet.format.event.Consumers.listElementsOf;
import static org.apache.parquet.format.event.Consumers.listOf;
import static org.apache.parquet.format.event.Consumers.struct;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.apache.parquet.format.event.Consumers.Consumer;
import org.apache.parquet.format.event.Consumers.DelegatingFieldConsumer;
import org.apache.parquet.format.event.EventBasedThriftReader;
import org.apache.parquet.format.event.TypedConsumer.I32Consumer;
import org.apache.parquet.format.event.TypedConsumer.I64Consumer;
import org.apache.parquet.format.event.TypedConsumer.StringConsumer;

/**
 * Utility to read/write metadata
 * We use the TCompactProtocol to serialize metadata
 */
public class Util {

  private final static int INIT_MEM_ALLOC_ENCR_BUFFER = 100;

  public static void writeColumnIndex(ColumnIndex columnIndex, OutputStream to) throws IOException {
    writeColumnIndex(columnIndex, to, null, null);
  }

  public static void writeColumnIndex(ColumnIndex columnIndex, OutputStream to, 
      BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    write(columnIndex, to, encryptor, AAD);
  }

  public static ColumnIndex readColumnIndex(InputStream from) throws IOException {
    return readColumnIndex(from, null, null);
  }

  public static ColumnIndex readColumnIndex(InputStream from, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    return read(from, new ColumnIndex(), decryptor, AAD);
  }

  public static void writeOffsetIndex(OffsetIndex offsetIndex, OutputStream to) throws IOException {
    writeOffsetIndex(offsetIndex, to, null, null);
  }

  public static void writeOffsetIndex(OffsetIndex offsetIndex, OutputStream to, 
      BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    write(offsetIndex, to, encryptor, AAD);
  }

  public static OffsetIndex readOffsetIndex(InputStream from) throws IOException {
    return readOffsetIndex(from, null, null);
  }

  public static OffsetIndex readOffsetIndex(InputStream from, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    return read(from, new OffsetIndex(), decryptor, AAD);
  }

  public static BloomFilterHeader readBloomFilterHeader(InputStream from) throws IOException {
    return readBloomFilterHeader(from, null, null);
  }

  public static void writeBloomFilterHeader(BloomFilterHeader header, OutputStream out) throws IOException {
    writeBloomFilterHeader(header, out, null, null);
  }
  
  public static BloomFilterHeader readBloomFilterHeader(InputStream from,
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    return read(from, new BloomFilterHeader(), decryptor, AAD);
  }

  public static void writeBloomFilterHeader(BloomFilterHeader header, OutputStream out,
      BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    write(header, out, encryptor, AAD);
  }

  public static void writePageHeader(PageHeader pageHeader, OutputStream to) throws IOException {
    writePageHeader(pageHeader, to, null, null);
  }

  public static void writePageHeader(PageHeader pageHeader, OutputStream to, 
      BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    write(pageHeader, to, encryptor, AAD);
  }

  public static PageHeader readPageHeader(InputStream from) throws IOException {
    return readPageHeader(from, null, null);
  }

  public static PageHeader readPageHeader(InputStream from, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    return validate(read(from, new PageHeader(), decryptor, AAD));
  }

  public static void writeFileMetaData(org.apache.parquet.format.FileMetaData fileMetadata, 
      OutputStream to) throws IOException {
    writeFileMetaData(fileMetadata, to, null, null);
  }

  public static void writeFileMetaData(org.apache.parquet.format.FileMetaData fileMetadata, 
      OutputStream to, BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    write(fileMetadata, to, encryptor, AAD);
  }

  public static FileMetaData readFileMetaData(InputStream from) throws IOException {
    return readFileMetaData(from, null, null);
  }

  public static FileMetaData readFileMetaData(InputStream from, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    return read(from, new FileMetaData(), decryptor, AAD);
  }

  public static void writeColumnMetaData(ColumnMetaData columnMetaData, OutputStream to, 
      BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    write(columnMetaData, to, encryptor, AAD);
  }

  public static ColumnMetaData readColumnMetaData(InputStream from, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    return read(from, new ColumnMetaData(), decryptor, AAD);
  }

  /**
   * reads the meta data from the stream
   * @param from the stream to read the metadata from
   * @param skipRowGroups whether row groups should be skipped
   * @return the resulting metadata
   * @throws IOException if any I/O error occurs during the reading
   */
  public static FileMetaData readFileMetaData(InputStream from, boolean skipRowGroups) throws IOException {
    return readFileMetaData(from, skipRowGroups, (BlockCipher.Decryptor) null, (byte[]) null);
  }

  public static FileMetaData readFileMetaData(InputStream from, boolean skipRowGroups, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    FileMetaData md = new FileMetaData();
    if (skipRowGroups) {
      readFileMetaData(from, new DefaultFileMetaDataConsumer(md), skipRowGroups, decryptor, AAD);
    } else {
      read(from, md, decryptor, AAD);
    }
    return md;
  }

  public static void writeFileCryptoMetaData(org.apache.parquet.format.FileCryptoMetaData cryptoMetadata, OutputStream to) throws IOException { 
    write(cryptoMetadata, to, null, null);
  }

  public static FileCryptoMetaData readFileCryptoMetaData(InputStream from) throws IOException {
    return read(from, new FileCryptoMetaData(), null, null);
  }

  /**
   * To read metadata in a streaming fashion.
   *
   */
  public static abstract class FileMetaDataConsumer {
    abstract public void setVersion(int version);
    abstract public void setSchema(List<SchemaElement> schema);
    abstract public void setNumRows(long numRows);
    abstract public void addRowGroup(RowGroup rowGroup);
    abstract public void addKeyValueMetaData(KeyValue kv);
    abstract public void setCreatedBy(String createdBy);
    abstract public void setEncryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm);
    abstract public void setFooterSigningKeyMetadata(byte[] footerSigningKeyMetadata);
  }

  /**
   * Simple default consumer that sets the fields
   *
   */
  public static final class DefaultFileMetaDataConsumer extends FileMetaDataConsumer {
    private final FileMetaData md;

    public DefaultFileMetaDataConsumer(FileMetaData md) {
      this.md = md;
    }

    @Override
    public void setVersion(int version) {
      md.setVersion(version);
    }

    @Override
    public void setSchema(List<SchemaElement> schema) {
      md.setSchema(schema);
    }

    @Override
    public void setNumRows(long numRows) {
      md.setNum_rows(numRows);
    }

    @Override
    public void setCreatedBy(String createdBy) {
      md.setCreated_by(createdBy);
    }

    @Override
    public void addRowGroup(RowGroup rowGroup) {
      md.addToRow_groups(rowGroup);
    }

    @Override
    public void addKeyValueMetaData(KeyValue kv) {
      md.addToKey_value_metadata(kv);
    }

    @Override
    public void setEncryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm) {
      md.setEncryption_algorithm(encryptionAlgorithm);
    }

    @Override
    public void setFooterSigningKeyMetadata(byte[] footerSigningKeyMetadata) {
      md.setFooter_signing_key_metadata(footerSigningKeyMetadata);
    }
  }

  public static void readFileMetaData(InputStream from, FileMetaDataConsumer consumer) throws IOException {
    readFileMetaData(from, consumer, null, null);
  }

  public static void readFileMetaData(InputStream from, FileMetaDataConsumer consumer, 
      BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    readFileMetaData(from, consumer, false, decryptor, AAD);
  }

  public static void readFileMetaData(InputStream from, final FileMetaDataConsumer consumer, boolean skipRowGroups) throws IOException {
    readFileMetaData(from, consumer, skipRowGroups, null, null);
  }

  public static void readFileMetaData(final InputStream input, final FileMetaDataConsumer consumer, 
      boolean skipRowGroups, BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    try {
      DelegatingFieldConsumer eventConsumer = fieldConsumer()
          .onField(VERSION, new I32Consumer() {
            @Override
            public void consume(int value) {
              consumer.setVersion(value);
            }
          }).onField(SCHEMA, listOf(SchemaElement.class, new Consumer<List<SchemaElement>>() {
            @Override
            public void consume(List<SchemaElement> schema) {
              consumer.setSchema(schema);
            }
          })).onField(NUM_ROWS, new I64Consumer() {
            @Override
            public void consume(long value) {
              consumer.setNumRows(value);
            }
          }).onField(KEY_VALUE_METADATA, listElementsOf(struct(KeyValue.class, new Consumer<KeyValue>() {
            @Override
            public void consume(KeyValue kv) {
              consumer.addKeyValueMetaData(kv);
            }
          }))).onField(CREATED_BY, new StringConsumer() {
            @Override
            public void consume(String value) {
              consumer.setCreatedBy(value);
            }
          }).onField(ENCRYPTION_ALGORITHM, struct(EncryptionAlgorithm.class, new Consumer<EncryptionAlgorithm>() {
            @Override
            public void consume(EncryptionAlgorithm encryptionAlgorithm) {
              consumer.setEncryptionAlgorithm(encryptionAlgorithm);
            }
          })).onField(FOOTER_SIGNING_KEY_METADATA, new StringConsumer() {
            @Override
            public void consume(String value) {
              byte[] keyMetadata = value.getBytes(StandardCharsets.UTF_8);
              consumer.setFooterSigningKeyMetadata(keyMetadata);
            }
          });

      if (!skipRowGroups) {
        eventConsumer = eventConsumer.onField(ROW_GROUPS, listElementsOf(struct(RowGroup.class, new Consumer<RowGroup>() {
          @Override
          public void consume(RowGroup rowGroup) {
            consumer.addRowGroup(rowGroup);
          }
        })));
      }

      final InputStream from;
      if (null == decryptor) {
        from = input;
      }
      else {
        byte[] plainText =  decryptor.decrypt(input, AAD);
        from = new ByteArrayInputStream(plainText);
      }
      new EventBasedThriftReader(protocol(from)).readStruct(eventConsumer);
    } catch (TException e) {
      throw new IOException("can not read FileMetaData: " + e.getMessage(), e);
    }
  }

  private static TProtocol protocol(OutputStream to) throws TTransportException {
    return protocol(new TIOStreamTransport(to));
  }

  private static TProtocol protocol(InputStream from) throws TTransportException {
    return protocol(new TIOStreamTransport(from));
  }

  private static InterningProtocol protocol(TIOStreamTransport t) {
    return new InterningProtocol(new TCompactProtocol(t));
  }


  private static <T extends TBase<?,?>> T read(final InputStream input, T tbase, BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
    final InputStream from;
    if (null == decryptor) {
      from = input;
    } else {
      byte[] plainText = decryptor.decrypt(input, AAD);
      from = new ByteArrayInputStream(plainText);
    }

    try {
      tbase.read(protocol(from));
      return tbase;
    } catch (TException e) {
      throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
    }
  }

  private static void write(TBase<?, ?> tbase, OutputStream to, BlockCipher.Encryptor encryptor, byte[] AAD) throws IOException {
    if (null == encryptor) { 
      try {
        tbase.write(protocol(to));
        return;
      } catch (TException e) {
        throw new IOException("can not write " + tbase, e);
      }
    }
    // Serialize and encrypt the structure
    try (TMemoryBuffer thriftMemoryBuffer = new TMemoryBuffer(INIT_MEM_ALLOC_ENCR_BUFFER)) {
      tbase.write(new InterningProtocol(new TCompactProtocol(thriftMemoryBuffer)));
      byte[] encryptedBuffer = encryptor.encrypt(thriftMemoryBuffer.getArray(), AAD);
      to.write(encryptedBuffer);
    } catch (TException e) {
      throw new IOException("can not write " + tbase, e);
    }
  }
}

