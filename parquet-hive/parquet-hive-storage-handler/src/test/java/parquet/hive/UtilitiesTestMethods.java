package parquet.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReader;
import parquet.column.page.mem.MemPageStore;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hive.writable.BinaryWritable;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

public class UtilitiesTestMethods {

  public static void writeToFile(final Path file, final Configuration configuration, final MessageType schema, final MemPageStore pageStore, final int recordCount)
          throws IOException {
    final ParquetFileWriter w = startFile(file, configuration, schema);
    writeBlock(schema, pageStore, recordCount, w);
    endFile(w);
  }

  public static void endFile(final ParquetFileWriter w) throws IOException {
    w.end(new HashMap<String, String>());
  }

  public static boolean smartCheckSchema(final GroupType expectedSchema, final GroupType actualSchema) {
    if (expectedSchema.getFieldCount() != actualSchema.getFieldCount()) {
      return false;
    }

    for (int i = 0; i < expectedSchema.getFieldCount(); ++i) {
      Type expectedType = expectedSchema.getType(i);
      Type actualType = actualSchema.getType(i);

      if (!expectedType.getName().equals(actualType.getName())
              || expectedType.getRepetition() != actualType.getRepetition()
              || expectedType.isPrimitive() != actualType.isPrimitive()) {
        return false;
      }

      if (expectedType.isPrimitive()) {
        if (expectedType.asPrimitiveType().getPrimitiveTypeName() != actualType.asPrimitiveType().getPrimitiveTypeName()
                || expectedType.asPrimitiveType().getTypeLength() != actualType.asPrimitiveType().getTypeLength()) {
          return false;
        }
      } else {
        if (!smartCheckSchema(expectedType.asGroupType(), actualType.asGroupType())) {
          return false;
        }
      }
    }

    return true;
  }

  public static boolean smartCheckArray(final Writable[] arrValue, final Writable[] arrExpected, final Integer[] arrCheckIndexValues) {

    int i = 0;
    for (final Integer index : arrCheckIndexValues) {
      if (index != Integer.MIN_VALUE) {
        final Writable value = arrValue[index];
        final Writable expectedValue = arrExpected[index];

        if (((value == null && expectedValue == null)
                || (((value != null && expectedValue != null) && (value.equals(expectedValue))))
                || (value != null && expectedValue != null && value instanceof ArrayWritable && expectedValue instanceof ArrayWritable && arrayWritableEquals((ArrayWritable) value, (ArrayWritable) expectedValue))) == false) {
          return false;
        }
      } else {
        final Writable value = arrValue[i];
        if (value != null) {
          return false;
        }
      }
      ++i;
    }

    return true;
  }

  public static boolean arrayWritableEquals(final ArrayWritable a1, final ArrayWritable a2) {
    final Writable[] a1Arr = a1.get();
    final Writable[] a2Arr = a2.get();

    if (a1Arr.length != a2Arr.length) {
      return false;
    }

    for (int i = 0; i < a1Arr.length; ++i) {
      if (a1Arr[i] instanceof ArrayWritable) {
        if (!(a2Arr[i] instanceof ArrayWritable)) {
          return false;
        }
        if (!arrayWritableEquals((ArrayWritable) a1Arr[i], (ArrayWritable) a2Arr[i])) {
          return false;
        }
      } else {
        if (!a1Arr[i].equals(a2Arr[i])) {
          return false;
        }
      }

    }
    return true;
  }

  static public ArrayWritable createArrayWritable(final Integer custkey, final String name, final String address, final Integer nationkey, final String phone, final Double acctbal, final String mktsegment, final String comment, final Map<String, String> map, final List<Integer> list) {

    final Writable[] arr = new Writable[11]; // The last one is for the unknown column
    arr[0] = new IntWritable(custkey);
    if (name != null) {
      arr[1] = new BinaryWritable(Binary.fromString(name));
    }
    if (address != null) {
      arr[2] = new BinaryWritable(Binary.fromString(address));
    }
    if (nationkey != null) {
      arr[3] = new IntWritable(nationkey);
    }
    if (phone != null) {
      arr[4] = new BinaryWritable(Binary.fromString(phone));
    }
    if (acctbal != null) {
      arr[5] = new DoubleWritable(acctbal);
    }
    if (mktsegment != null) {
      arr[6] = new BinaryWritable(Binary.fromString(mktsegment));
    }
    if (comment != null) {
      arr[7] = new BinaryWritable(Binary.fromString(comment));
    }
    if (map != null) {
      final Writable[] mapContainer = new Writable[1];
      final Writable[] mapArr = new Writable[map.size()];
      int i = 0;
      for (Map.Entry<String, String> entry : map.entrySet()) {
        final Writable[] pair = new Writable[2];
        pair[0] = new BinaryWritable(Binary.fromString(entry.getKey()));
        pair[1] = new BinaryWritable(Binary.fromString(entry.getValue()));
        mapArr[i] = new ArrayWritable(Writable.class, pair);
        ++i;
      }
      mapContainer[0] = new ArrayWritable(Writable.class, mapArr);
      arr[8] = new ArrayWritable(Writable.class, mapContainer);
    }
    if (list != null) {
      final Writable[] listContainer = new Writable[1];
      final Writable[] listArr = new Writable[list.size()];
      for (int i = 0; i < list.size(); ++i) {
        listArr[i] = new IntWritable(list.get(i));
      }
      listContainer[0] = new ArrayWritable(Writable.class, listArr);
      arr[9] = new ArrayWritable(Writable.class, listContainer);
    }

    return new ArrayWritable(Writable.class, arr);
  }

  public static void writeBlock(final MessageType schema, final MemPageStore pageStore,
          final int recordCount, final ParquetFileWriter w) throws IOException {
    w.startBlock(recordCount);
    final List<ColumnDescriptor> columns = schema.getColumns();
    for (final ColumnDescriptor columnDescriptor : columns) {
      final PageReader pageReader = pageStore.getPageReader(columnDescriptor);
      final long totalValueCount = pageReader.getTotalValueCount();
      w.startColumn(columnDescriptor, totalValueCount, CompressionCodecName.UNCOMPRESSED);
      int n = 0;
      do {
        final Page page = pageReader.readPage();
        n += page.getValueCount();
        // TODO: change INTFC
        w.writeDataPage(
                page.getValueCount(),
                (int) page.getBytes().size(),
                BytesInput.from(page.getBytes().toByteArray()),
                page.getRlEncoding(),
                page.getDlEncoding(),
                page.getValueEncoding());
      } while (n < totalValueCount);
      w.endColumn();
    }
    w.endBlock();
  }

  public static ParquetFileWriter startFile(final Path file,
          final Configuration configuration, final MessageType schema) throws IOException {
    final ParquetFileWriter w = new ParquetFileWriter(configuration, schema, file);
    w.start();
    return w;
  }

  public static void writeField(final RecordConsumer recordWriter, final int index, final String name, final Object value) {
    if (value != null) {
      recordWriter.startField(name, index);
      if (value instanceof Integer) {
        recordWriter.addInteger((Integer) value);
      } else if (value instanceof String) {
        recordWriter.addBinary(Binary.fromString((String) value));
      } else if (value instanceof Double) {
        recordWriter.addDouble((Double) value);
      } else if (value instanceof Map) {
        recordWriter.startGroup();
        recordWriter.startField("map", 0);
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          recordWriter.startGroup();
          writeField(recordWriter, 0, "key", entry.getKey());
          writeField(recordWriter, 1, "value", entry.getValue());
          recordWriter.endGroup();
        }
        recordWriter.endField("map", 0);
        recordWriter.endGroup();
      } else if (value instanceof List) {
        recordWriter.startGroup();
        recordWriter.startField("bag", 0);
        for (Object element : (List<?>) value) {
          recordWriter.startGroup();
          writeField(recordWriter, 0, "array_element", element);
          recordWriter.endGroup();
        }
        recordWriter.endField("bag", 0);
        recordWriter.endGroup();
      } else {
        throw new IllegalArgumentException(value.getClass().getName() + " not supported");
      }

      recordWriter.endField(name, index);
    }
  }
}
