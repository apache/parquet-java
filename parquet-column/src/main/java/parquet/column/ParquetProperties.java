package parquet.column;

import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesWriter;
import parquet.column.values.boundedint.DevNullValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
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
      for(WriterVersion v : WriterVersion.values()) {
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
          BytesUtils.getWidthFromMaxInt(maxLevel), initialSizePerCol);
    }
  }

  public ValuesWriter getValuesWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch (path.getType()) {
    case BOOLEAN:
      if(writerVersion == WriterVersion.PARQUET_1_0) {
        return new BooleanPlainValuesWriter();
      } else if (writerVersion == WriterVersion.PARQUET_2_0) {
        return new RunLengthBitPackingHybridValuesWriter(1, initialSizePerCol);
      }
      break;
    case BINARY:
      if(enableDictionary) {
        return new PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
      } else {
        if (writerVersion == WriterVersion.PARQUET_1_0) {
          return new PlainValuesWriter(initialSizePerCol);
        } else if (writerVersion == WriterVersion.PARQUET_2_0) {
          // TODO enable 2.0 encodings in another commit
          // return new DeltaByteArrayWriter(initialSizePerCol);
          return new PlainValuesWriter(initialSizePerCol);
        } 
      }
      break;
    case INT32:
      if(enableDictionary) {
        return new PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
      } else {
        if(writerVersion == WriterVersion.PARQUET_1_0) {
          return new PlainValuesWriter(initialSizePerCol);
        } else if(writerVersion == WriterVersion.PARQUET_2_0) {
          // TODO enable 2.0 encodings in another commit
          // return new DeltaBinaryPackingValuesWriter(initialSizePerCol);
          return new PlainValuesWriter(initialSizePerCol);
        }
      }
      break;
    case INT64:
      if(enableDictionary) {
        return new PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
      } else {
        return new PlainValuesWriter(initialSizePerCol);
      }
    case DOUBLE:
      if(enableDictionary) {
        return new PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
      } else {
        return new PlainValuesWriter(initialSizePerCol);
      }
    case FLOAT:
      if(enableDictionary) {
        return new PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
      } else {
        return new PlainValuesWriter(initialSizePerCol);
      }
    case FIXED_LEN_BYTE_ARRAY:
      return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol);
    default:
      return new PlainValuesWriter(initialSizePerCol);
    }
    return null;
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
