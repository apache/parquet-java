package parquet.column;

import parquet.bytes.ByteBufferAllocator;
import parquet.bytes.BytesUtils;
import parquet.bytes.HeapByteBufferAllocator;
import parquet.column.values.ValuesWriter;
import parquet.column.values.boundedint.DevNullValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
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
  private ByteBufferAllocator allocator;

  public ParquetProperties(int dictPageSize, WriterVersion writerVersion, boolean enableDict) {
    this(dictPageSize, writerVersion, enableDict, new HeapByteBufferAllocator());
  }

  public ParquetProperties(int dictPageSize, WriterVersion writerVersion, boolean enableDict, ByteBufferAllocator allocator) {
    this.dictionaryPageSizeThreshold = dictPageSize;
    this.writerVersion = writerVersion;
    this.enableDictionary = enableDict;
    this.allocator=allocator;
  }
  
  public static ValuesWriter getColumnDescriptorValuesWriter(int maxLevel,  int initialSizePerCol, ByteBufferAllocator allocator) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(
          BytesUtils.getWidthFromMaxInt(maxLevel), initialSizePerCol, allocator!=null?allocator:new HeapByteBufferAllocator());
    }
  }

  public ValuesWriter getValuesWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch (path.getType()) {
    case BOOLEAN:
      if(writerVersion == WriterVersion.PARQUET_1_0) {
        return new BooleanPlainValuesWriter(this.allocator);
      } else if (writerVersion == WriterVersion.PARQUET_2_0) {
        return new RunLengthBitPackingHybridValuesWriter(1, initialSizePerCol, this.allocator);
      }
      break;
    case BINARY:
      if(enableDictionary) {
        return new PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, this.allocator);
      } else {
        if (writerVersion == WriterVersion.PARQUET_1_0) {
          return new PlainValuesWriter(initialSizePerCol, this.allocator);
        } else if (writerVersion == WriterVersion.PARQUET_2_0) {
          return new DeltaByteArrayWriter(initialSizePerCol, this.allocator);
        } 
      }
      break;
    case INT32:
      if(enableDictionary) {
        return new PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, this.allocator);
      } else {
        if(writerVersion == WriterVersion.PARQUET_1_0) {
          return new PlainValuesWriter(initialSizePerCol, this.allocator);
        } else if(writerVersion == WriterVersion.PARQUET_2_0) {
          return new DeltaBinaryPackingValuesWriter(initialSizePerCol, this.allocator);
        }
      }
      break;
    case INT64:
      if(enableDictionary) {
        return new PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, this.allocator);
      } else {
        return new PlainValuesWriter(initialSizePerCol, this.allocator);
      }
    case INT96:
      if (enableDictionary) {
        return new PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, 12, this.allocator);
      } else {
        return new FixedLenByteArrayPlainValuesWriter(12, initialSizePerCol, this.allocator);
      }
    case DOUBLE:
      if(enableDictionary) {
        return new PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, this.allocator);
      } else {
        return new PlainValuesWriter(initialSizePerCol, this.allocator);
      }
    case FLOAT:
      if(enableDictionary) {
        return new PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, this.allocator);
      } else {
        return new PlainValuesWriter(initialSizePerCol, this.allocator);
      }
    case FIXED_LEN_BYTE_ARRAY:
      if (enableDictionary && (writerVersion == WriterVersion.PARQUET_2_0)) {
        return new PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol, path.getTypeLength(), this.allocator);
      } else {
        return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol, this.allocator);
      }
    default:
      return new PlainValuesWriter(initialSizePerCol, this.allocator);
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
