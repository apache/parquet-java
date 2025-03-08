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
package org.apache.parquet.hadoop.metadata;

import static org.apache.parquet.format.Util.readColumnMetaData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.schema.PrimitiveType;

class EncryptedColumnChunkMetaData extends ColumnChunkMetaData {
  private final ParquetMetadataConverter parquetMetadataConverter;
  private final byte[] encryptedMetadata;
  private final byte[] columnKeyMetadata;
  private final InternalFileDecryptor fileDecryptor;

  private final int columnOrdinal;
  private final PrimitiveType primitiveType;
  private final String createdBy;
  private final ColumnPath path;

  private boolean decrypted;
  private ColumnChunkMetaData shadowColumnChunkMetaData;

  private EncryptedColumnChunkMetaData(Builder builder) {
    super(builder);
    this.parquetMetadataConverter = builder.parquetMetadataConverter;
    this.path = builder.path;
    this.encryptedMetadata = builder.encryptedMetadata;
    this.columnKeyMetadata = builder.columnKeyMetadata;
    this.fileDecryptor = builder.fileDecryptor;
    this.columnOrdinal = builder.columnOrdinal;
    this.primitiveType = builder.primitiveType;
    this.createdBy = builder.createdBy;
    this.decrypted = false;
    this.rowGroupOrdinal = builder.rowGroupOrdinal;
  }

  @Override
  protected void decryptIfNeeded() {
    if (decrypted) return;

    if (null == fileDecryptor) {
      throw new ParquetCryptoRuntimeException(path + ". Null File Decryptor");
    }

    // Decrypt the ColumnMetaData
    InternalColumnDecryptionSetup columnDecryptionSetup =
        fileDecryptor.setColumnCryptoMetadata(path, true, false, columnKeyMetadata, columnOrdinal);

    ColumnMetaData metaData;
    ByteArrayInputStream tempInputStream = new ByteArrayInputStream(encryptedMetadata);
    byte[] columnMetaDataAAD = AesCipher.createModuleAAD(
        fileDecryptor.getFileAAD(),
        ModuleCipherFactory.ModuleType.ColumnMetaData,
        rowGroupOrdinal,
        columnOrdinal,
        -1);
    try {
      metaData = readColumnMetaData(
          tempInputStream, columnDecryptionSetup.getMetaDataDecryptor(), columnMetaDataAAD);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException(path + ". Failed to decrypt column metadata", e);
    }
    decrypted = true;
    shadowColumnChunkMetaData =
        parquetMetadataConverter.buildColumnChunkMetaData(metaData, path, primitiveType, createdBy);
    this.encodingStats = shadowColumnChunkMetaData.encodingStats;
    this.properties = shadowColumnChunkMetaData.properties;
    if (metaData.isSetBloom_filter_offset()) {
      setBloomFilterOffset(metaData.getBloom_filter_offset());
    }
    if (metaData.isSetBloom_filter_length()) {
      setBloomFilterLength(metaData.getBloom_filter_length());
    }
  }

  @Override
  public ColumnPath getPath() {
    return path;
  }

  @Override
  public long getFirstDataPageOffset() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getFirstDataPageOffset();
  }

  @Override
  public long getDictionaryPageOffset() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getDictionaryPageOffset();
  }

  @Override
  public long getValueCount() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getValueCount();
  }

  @Override
  public long getTotalUncompressedSize() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getTotalUncompressedSize();
  }

  @Override
  public long getTotalSize() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getTotalSize();
  }

  @Override
  public Statistics getStatistics() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getStatistics();
  }

  @Override
  public SizeStatistics getSizeStatistics() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getSizeStatistics();
  }

  /**
   * @return whether or not this column is encrypted
   */
  @Override
  public boolean isEncrypted() {
    return true;
  }

  public static class Builder extends ColumnChunkMetaData.Builder<Builder> {
    private ParquetMetadataConverter parquetMetadataConverter;
    private ColumnPath path;
    private byte[] encryptedMetadata;
    private byte[] columnKeyMetadata;
    private InternalFileDecryptor fileDecryptor;
    private int columnOrdinal;
    private PrimitiveType primitiveType;
    private String createdBy;
    private int rowGroupOrdinal;

    public Builder withParquetMetadataConverter(ParquetMetadataConverter parquetMetadataConverter) {
      this.parquetMetadataConverter = parquetMetadataConverter;
      return this;
    }

    public Builder withPath(ColumnPath path) {
      this.path = path;
      return this;
    }

    public Builder withEncryptedMetadata(byte[] encryptedMetadata) {
      this.encryptedMetadata = encryptedMetadata;
      return this;
    }

    public Builder withColumnKeyMetadata(byte[] columnKeyMetadata) {
      this.columnKeyMetadata = columnKeyMetadata;
      return this;
    }

    public Builder withFileDecryptor(InternalFileDecryptor fileDecryptor) {
      this.fileDecryptor = fileDecryptor;
      return this;
    }

    public Builder withColumnOrdinal(int columnOrdinal) {
      this.columnOrdinal = columnOrdinal;
      return this;
    }

    public Builder withPrimitiveType(PrimitiveType primitiveType) {
      this.primitiveType = primitiveType;
      return this;
    }

    public Builder withCreatedBy(String createdBy) {
      this.createdBy = createdBy;
      return this;
    }

    public Builder withRowGroupOrdinal(int rowGroupOrdinal) {
      this.rowGroupOrdinal = rowGroupOrdinal;
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public ColumnChunkMetaData build() {
      return new EncryptedColumnChunkMetaData(this);
    }
  }
}
