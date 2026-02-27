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
package org.apache.parquet.cli.commands;

import static org.apache.parquet.cli.Util.encodingsAsString;
import static org.apache.parquet.cli.Util.primitive;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.commons.text.TextStringBuilder;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;

/**
 * Prints encryption information for a Parquet file.
 * <p>
 * The command never attempts to decrypt data. If the footer is encrypted it
 * simply reports that fact.
 */
@Parameters(commandDescription = "Print Parquet file encryption details")
public class EncryptionInfoCommand extends BaseCommand {

  public EncryptionInfoCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>", required = true)
  List<String> targets;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(targets.size() == 1, "Exactly one Parquet file must be specified.");
    String source = targets.get(0);

    ParquetMetadata footer =
        ParquetFileReader.readFooter(getConf(), qualifiedPath(source), ParquetMetadataConverter.NO_FILTER);

    FileMetaData meta = footer.getFileMetaData();
    console.info("File: {}", source);
    console.info("Encryption type: {}", meta.getEncryptionType());

    if (meta.getEncryptionType() == EncryptionType.ENCRYPTED_FOOTER) {
      console.info("Footer is encrypted – column list unavailable without keys.");
      return 0;
    }

    ColumnEncryptionInfo encryptionInfo = analyzeColumnEncryption(footer);

    console.info("Column encryption summary:");
    console.info("  Total columns: {}", encryptionInfo.getTotalColumns());
    console.info(
        "  Encrypted columns: {}", encryptionInfo.getEncryptedColumns().size());
    console.info(
        "  Unencrypted columns: {}",
        encryptionInfo.getUnencryptedColumns().size());

    if (!encryptionInfo.getEncryptedColumns().isEmpty()) {
      console.info("  Encrypted columns:");
      for (String col : encryptionInfo.getEncryptedColumns()) {
        console.info("    {}", col);
      }
    }

    if (!footer.getBlocks().isEmpty()) {
      console.info("\nColumns:");
      listColumnEncryptionStatus(console, footer);
    }

    return 0;
  }

  @Override
  public java.util.List<String> getExamples() {
    return java.util.Collections.singletonList("encryption-info sample.parquet");
  }

  private void listColumnEncryptionStatus(Logger console, ParquetMetadata footer) {
    BlockMetaData firstRowGroup = footer.getBlocks().get(0);

    int maxColumnWidth =
        maxSize(Iterables.transform(firstRowGroup.getColumns(), new Function<ColumnChunkMetaData, String>() {
          @Override
          public String apply(@Nullable ColumnChunkMetaData input) {
            return input == null ? "" : input.getPath().toDotString();
          }
        }));

    console.info(
        String.format("%-" + maxColumnWidth + "s  %-9s %-9s %s", "Column", "Encrypted", "Type", "Encodings"));
    console.info(new TextStringBuilder(maxColumnWidth + 40)
        .appendPadding(maxColumnWidth + 40, '-')
        .toString());

    MessageType schema = footer.getFileMetaData().getSchema();

    for (ColumnChunkMetaData column : firstRowGroup.getColumns()) {
      String name = column.getPath().toDotString();
      String status = column.isEncrypted() ? "ENCRYPTED" : "-";

      PrimitiveType ptype = primitive(schema, column.getPath().toArray());
      String typeStr = ptype.getPrimitiveTypeName().name();

      String enc = encodingsAsString(
          column.getEncodings(),
          schema.getColumnDescription(column.getPath().toArray()));

      console.info(String.format("%-" + maxColumnWidth + "s  %-9s %-9s %s", name, status, typeStr, enc));
    }
  }

  private int maxSize(Iterable<String> strings) {
    int size = 0;
    for (String s : strings) {
      size = Math.max(size, s.length());
    }
    return size;
  }

  /** Visible for test */
  static Set<String> findEncryptedColumns(ParquetMetadata footer) {
    Set<String> cols = new TreeSet<>();
    for (BlockMetaData rg : footer.getBlocks()) {
      for (ColumnChunkMetaData c : rg.getColumns()) {
        if (c.isEncrypted()) {
          cols.add(c.getPath().toDotString());
        }
      }
    }
    return cols;
  }

  private ColumnEncryptionInfo analyzeColumnEncryption(ParquetMetadata footer) {
    Set<String> encryptedColumns = new TreeSet<>();
    Set<String> unencryptedColumns = new TreeSet<>();

    for (BlockMetaData rg : footer.getBlocks()) {
      for (ColumnChunkMetaData c : rg.getColumns()) {
        String columnName = c.getPath().toDotString();
        if (c.isEncrypted()) {
          encryptedColumns.add(columnName);
        } else {
          unencryptedColumns.add(columnName);
        }
      }
    }

    return new ColumnEncryptionInfo(encryptedColumns, unencryptedColumns);
  }

  private static class ColumnEncryptionInfo {
    private final Set<String> encryptedColumns;
    private final Set<String> unencryptedColumns;

    public ColumnEncryptionInfo(Set<String> encryptedColumns, Set<String> unencryptedColumns) {
      this.encryptedColumns = encryptedColumns;
      this.unencryptedColumns = unencryptedColumns;
    }

    public Set<String> getEncryptedColumns() {
      return encryptedColumns;
    }

    public Set<String> getUnencryptedColumns() {
      return unencryptedColumns;
    }

    public int getTotalColumns() {
      return encryptedColumns.size() + unencryptedColumns.size();
    }
  }
}
