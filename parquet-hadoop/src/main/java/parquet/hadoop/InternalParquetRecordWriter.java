package parquet.hadoop;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;

import parquet.schema.MessageType;

abstract class InternalParquetRecordWriter<T> {

  static final int MINIMUM_BUFFER_SIZE = 64 * 1024;
  static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;
  final int initialBlockBufferSize;
  final int initialPageBufferSize;

  InternalParquetRecordWriter(MessageType schema, int rowGroupSize, int pageSize) {
    // we don't want this number to be too small
    // ideally we divide the block equally across the columns
    // it is unlikely all columns are going to be the same size.
    this.initialBlockBufferSize = max(MINIMUM_BUFFER_SIZE, rowGroupSize / schema.getColumns().size() / 5);
    // we don't want this number to be too small either
    // ideally, slightly bigger than the page size, but not bigger than the block buffer
    this.initialPageBufferSize = max(MINIMUM_BUFFER_SIZE, min(pageSize + pageSize / 10, initialBlockBufferSize));
  }

  abstract void close() throws IOException;

  abstract void write(T value) throws IOException;

}