/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.Util;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import java.io.IOException;
import java.util.List;

// TODO: show dictionary size in values and in bytes
@Parameters(commandDescription="Print dictionaries for a Parquet column")
public class ShowDictionaryCommand extends BaseCommand {

  public ShowDictionaryCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  List<String> targets;

  @Parameter(
      names = {"-c", "--column"},
      description = "Column path",
      required = true)
  String column;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() >= 1,
        "A Parquet file is required.");
    Preconditions.checkArgument(targets.size() == 1,
        "Cannot process multiple Parquet files.");

    String source = targets.get(0);

    ParquetFileReader reader = ParquetFileReader.open(getConf(), qualifiedPath(source));
    MessageType schema = reader.getFileMetaData().getSchema();
    ColumnDescriptor descriptor = Util.descriptor(column, schema);
    PrimitiveType type = Util.primitive(column, schema);
    Preconditions.checkNotNull(type);

    DictionaryPageReadStore dictionaryReader;
    int rowGroup = 0;
    while ((dictionaryReader = reader.getNextDictionaryReader()) != null) {
      DictionaryPage page = dictionaryReader.readDictionaryPage(descriptor);

      Dictionary dict = page.getEncoding().initDictionary(descriptor, page);

      console.info("\nRow group {} dictionary for \"{}\":", rowGroup, column, page.getCompressedSize());
      for (int i = 0; i <= dict.getMaxId(); i += 1) {
        switch(type.getPrimitiveTypeName()) {
          case BINARY:
            if (type.getOriginalType() == OriginalType.UTF8) {
              console.info("{}: {}", String.format("%6d", i),
                  Util.humanReadable(dict.decodeToBinary(i).toStringUsingUTF8(), 70));
            } else {
              console.info("{}: {}", String.format("%6d", i),
                  Util.humanReadable(dict.decodeToBinary(i).getBytesUnsafe(), 70));
            }
            break;
          case INT32:
            console.info("{}: {}", String.format("%6d", i),
              dict.decodeToInt(i));
            break;
          case INT64:
            console.info("{}: {}", String.format("%6d", i),
                dict.decodeToLong(i));
            break;
          case FLOAT:
            console.info("{}: {}", String.format("%6d", i),
                dict.decodeToFloat(i));
            break;
          case DOUBLE:
            console.info("{}: {}", String.format("%6d", i),
                dict.decodeToDouble(i));
            break;
          default:
            throw new IllegalArgumentException(
                "Unknown dictionary type: " + type.getPrimitiveTypeName());
        }
      }

      reader.skipNextRowGroup();

      rowGroup += 1;
    }

    console.info("");

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Show the dictionary for column 'col' from a Parquet file",
        "-c col sample.parquet"
    );
  }
}
