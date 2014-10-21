package parquet.column;

import static parquet.bytes.BytesUtils.getWidthFromMaxInt;
import parquet.column.values.ValuesWriter;
import parquet.column.values.boundedint.DevNullValuesWriter;
import parquet.column.values.fallback.FallbackValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import parquet.column.values.plain.BooleanPlainValuesWriter;
import parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

/**
 * This class represents all the configurable Parquet properties.
 *
 * @author amokashi
 *
 */
public class ParquetProperties {

  public enum WriterVersion {
    PARQUET_1_0 ("v1"),
    PARQUET_2_0 ("v2");

    private final String shortName;

    WriterVersion(String shortname) {
      this.shortName = shortname;
    }

    public static WriterVersion fromString(String name) {
      for (WriterVersion v : WriterVersion.values()) {
        if (v.shortName.equals(name)) {
          return v;
        }
      }
      // Throws IllegalArgumentException if name does not exact match with enum name
      return WriterVersion.valueOf(name);
    }
  }
  private final int dictionaryPageSizeThreshold;
  private final WriterVersion writerVersion;
  private final boolean enableDictionary;

  public ParquetProperties(int dictPageSize, WriterVersion writerVersion, boolean enableDict) {
    this.dictionaryPageSizeThreshold = dictPageSize;
    this.writerVersion = writerVersion;
    this.enableDictionary = enableDict;
  }

  public static ValuesWriter getColumnDescriptorValuesWriter(int maxLevel,  int initialSizePerCol) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(
          getWidthFromMaxInt(maxLevel), initialSizePerCol);
    }
  }

  private ValuesWriter plainWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch (path.getType()) {
    case BOOLEAN:
      return new BooleanPlainValuesWriter();
    case INT96:
      return new FixedLenByteArrayPlainValuesWriter(12, initialSizePerCol);
    case FIXED_LEN_BYTE_ARRAY:
      return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol);
    case BINARY:
    case INT32:
    case INT64:
    case DOUBLE:
    case FLOAT:
      return new PlainValuesWriter(initialSizePerCol);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  private DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch (path.getType()) {
    case BOOLEAN:
      throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
    case BINARY:
      return new PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold);
    case INT32:
      return new PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold);
    case INT64:
      return new PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold);
    case INT96:
      return new PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, 12);
    case DOUBLE:
      return new PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold);
    case FLOAT:
      return new PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold);
    case FIXED_LEN_BYTE_ARRAY:
      return new PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, path.getTypeLength());
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  private ValuesWriter fallbackWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch(writerVersion) {
    case PARQUET_1_0:
      return plainWriter(path, initialSizePerCol);
    case PARQUET_2_0:
      switch (path.getType()) {
      case BOOLEAN:
        return new RunLengthBitPackingHybridValuesWriter(1, initialSizePerCol);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return new DeltaByteArrayWriter(initialSizePerCol);
      case INT32:
        return new DeltaBinaryPackingValuesWriter(initialSizePerCol);
      case INT96:
      case INT64:
      case DOUBLE:
      case FLOAT:
        return plainWriter(path, initialSizePerCol);
      default:
        throw new IllegalArgumentException("Unknown type " + path.getType());
      }
    default:
      throw new IllegalArgumentException("Unknown version: " + writerVersion);
    }
  }

  private ValuesWriter dictWriterWithFallBack(ColumnDescriptor path, int initialSizePerCol) {
    ValuesWriter fallbackWriter = fallbackWriter(path, initialSizePerCol);
    if (enableDictionary) {
      return FallbackValuesWriter.of(
          dictionaryWriter(path, initialSizePerCol),
          fallbackWriter);
    } else {
     return fallbackWriter;
    }
  }

  public ValuesWriter getValuesWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch (path.getType()) {
    case BOOLEAN: // no dictionary encoding for boolean
      return fallbackWriter(path, initialSizePerCol);
    case FIXED_LEN_BYTE_ARRAY:
      // dictionary encoding for that type was not enabled in PARQUET 1.0
      if (writerVersion == WriterVersion.PARQUET_2_0) {
        return dictWriterWithFallBack(path, initialSizePerCol);
      } else {
       return fallbackWriter(path, initialSizePerCol);
      }
    case BINARY:
    case INT32:
    case INT64:
    case INT96:
    case DOUBLE:
    case FLOAT:
      return dictWriterWithFallBack(path, initialSizePerCol);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  public int getDictionaryPageSizeThreshold() {
    return dictionaryPageSizeThreshold;
  }

  public WriterVersion getWriterVersion() {
    return writerVersion;
  }

  public boolean isEnableDictionary() {
    return enableDictionary;
  }
}
