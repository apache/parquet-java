/**
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.ValuesType;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;

/**
 * Utility to check pages metadata
 * @author Julien Le Dem
 *
 */
public class CheckPages {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("usage CheckPages <path> [<col>]*");
      return;
    }
    Path path = new Path(new URI(args[0]));
    final Configuration configuration = new Configuration();

    final FileSystem fs = path.getFileSystem(configuration);
    FileStatus fileStatus = fs.getFileStatus(path);
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, fileStatus);
    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    if (args.length > 1) {
      Set<String> requested = new HashSet<String>();
      for (int i = 1; i < args.length; i++) {
        requested.add(args[i]);
      }
      Iterator<ColumnDescriptor> iterator = columns.iterator();
      while (iterator.hasNext()) {
        if (!requested.contains(dotted(iterator.next().getPath()))) {
          iterator.remove();
        }
      }
    }
    Map<ColumnDescriptor, Counts> totalCounts = new HashMap<ColumnDescriptor, Counts>();
    for (ColumnDescriptor col : columns) {
      totalCounts.put(col, new Counts());
    }

    System.out.println(footer.getBlocks().size() + " blocks");
    int i = 0;
    for (BlockMetaData blockMetaData : footer.getBlocks()) {
      System.out.println("block " + i);
      ++ i;
      List<BlockMetaData> blocks = new ArrayList<BlockMetaData>(1);
      blocks.add(blockMetaData);
      ParquetFileReader reader = new ParquetFileReader(configuration, path, blocks, columns);
      PageReadStore pages = reader.readNextRowGroup();
      for (ColumnDescriptor col : columns) {
        PageReader pageReader = pages.getPageReader(col);
        System.out.println("col " + col + ": values=" + pageReader.getTotalValueCount() + " rows=" + pages.getRowCount());
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        Dictionary dictionary = null;
        if (dictionaryPage != null) {
          System.out.println("Dic: " + dictionaryPage);
          dictionary = dictionaryPage.getEncoding().initDictionary(col, dictionaryPage);
        }
        Counts counts = totalCounts.get(col);
        Page page;
        long totalValues = 0;
        long recordCount = 0;
        int nulls = 0;
        int p = 0;
        while ((page = pageReader.readPage()) != null) {
          System.out.println("page " + p + ": size=" + page.getBytes().size() + " valueCount=" + page.getValueCount() + " uncompressedSize=" + page.getUncompressedSize());
          ++ p;
          int pageValueCount = page.getValueCount();
          totalValues += pageValueCount;
          ValuesReader repetitionLevelColumn = page.getRlEncoding().getValuesReader(col, ValuesType.REPETITION_LEVEL);
          ValuesReader definitionLevelColumn = page.getDlEncoding().getValuesReader(col, ValuesType.DEFINITION_LEVEL);
          final ValuesReader dataColumn;
          if (page.getValueEncoding().usesDictionary()) {
            dataColumn = page.getValueEncoding().getDictionaryBasedValuesReader(col, ValuesType.VALUES, dictionary);
          } else {
            dataColumn = page.getValueEncoding().getValuesReader(col, ValuesType.VALUES);
          }
          byte[] bytes = page.getBytes().toByteArray();
          repetitionLevelColumn.initFromPage(pageValueCount, bytes, 0);
          int next = repetitionLevelColumn.getNextOffset();
          definitionLevelColumn.initFromPage(pageValueCount, bytes, next);
          next = definitionLevelColumn.getNextOffset();
          dataColumn.initFromPage(pageValueCount, bytes, next);
          PrimitiveTypeNameConverter<Object, RuntimeException> converter = new PrimitiveTypeNameConverter<Object, RuntimeException>() {
            @Override public Object convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readFloat();
            }
            @Override public Object convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readDouble();
            }
            @Override public Object convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readInteger();
            }
            @Override public Object convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readLong();
            }
            @Override public Object convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readBytes();
            }
            @Override public Object convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readBytes();
            }
            @Override public Object convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readBoolean();
            }
            @Override public Object convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return dataColumn.readBytes();
            }
          };
          for (int j = 0; j < pageValueCount; j++) {
            int r = repetitionLevelColumn.readInteger();
            if (r == 0) {
              ++ recordCount;
            }
            int d = definitionLevelColumn.readInteger();
//            System.out.println("r = " + r + " d = " + d);
            if (d == col.getMaxDefinitionLevel()) {
              Object result = col.getType().convert(converter);
//              System.out.println(result);
            } else {
              ++ nulls;
            }
          }
        }
        counts.totalRowsInRowGroupMetadata += pages.getRowCount();
        counts.totalRowsInData += recordCount;
        counts.totalValuesInRowGroupMetadata += pageReader.getTotalValueCount();
        counts.totalValuesInPageMetadata += totalValues;
        System.out.println("nulls: " + nulls);
        if (pages.getRowCount() != recordCount) {
          System.out.println(col + "!!!!!! " + recordCount + " != " + pages.getRowCount() + " missing " + (pages.getRowCount() - recordCount));
        }
        if (totalValues != pageReader.getTotalValueCount()) {
          System.out.println(col + "!!!!!! " + totalValues + " != " + pageReader.getTotalValueCount());
        }
      }
    }
    System.out.println("totals:");
    for (ColumnDescriptor col : columns) {
      System.out.println(col + " " + totalCounts.get(col));
    }

  }

  private static class Counts {
    public long totalValuesInPageMetadata;
    public long totalValuesInRowGroupMetadata;
    public long totalRowsInData;
    public long totalRowsInRowGroupMetadata;
    @Override
    public String toString() {
      String rows = (totalRowsInData == totalRowsInRowGroupMetadata) ? String.valueOf(totalRowsInData) :
        "data = " + totalRowsInData + " row groups = " + totalRowsInRowGroupMetadata + " diff = " + (totalRowsInData - totalRowsInRowGroupMetadata);
      String values = (totalValuesInPageMetadata == totalValuesInRowGroupMetadata) ? String.valueOf(totalValuesInPageMetadata) :
        "pages = " + totalValuesInPageMetadata + " row groups = " + totalValuesInRowGroupMetadata + " diff = " + (totalValuesInPageMetadata - totalValuesInRowGroupMetadata);
      return "values: " + values + ", rows: " + rows;
    }
  }

  private static String dotted(String[] path) {
    String result = "";
    for (int i = 0; i < path.length; i++) {
      if (i != 0) {
        result += ".";
      }
      result += path[i];
    }
    return result;
  }
}