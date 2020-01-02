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
package org.apache.parquet.column.values.delta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.Before;
import org.junit.Test;

import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetDecodingException;

public class DeltaBinaryPackingValuesWriterForLongTest {
  DeltaBinaryPackingValuesReader reader;
  private int blockSize;
  private int miniBlockNum;
  private ValuesWriter writer;
  private Random random;

  @Before
  public void setUp() {
    blockSize = 128;
    miniBlockNum = 4;
    writer = new DeltaBinaryPackingValuesWriterForLong(
        blockSize, miniBlockNum, 100, 200, new DirectByteBufferAllocator());
    random = new Random(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void miniBlockSizeShouldBeMultipleOf8() {
    new DeltaBinaryPackingValuesWriterForLong(
        1281, 4, 100, 100, new DirectByteBufferAllocator());
  }

  /* When data size is multiple of Block */
  @Test
  public void shouldWriteWhenDataIsAlignedWithBlock() throws IOException {
    long[] data = new long[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = random.nextLong();
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldWriteAndReadWhenBlockIsNotFullyWritten() throws IOException {
    long[] data = new long[blockSize - 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextLong();
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldWriteAndReadWhenAMiniBlockIsNotFullyWritten() throws IOException {
    int miniBlockSize = blockSize / miniBlockNum;
    long[] data = new long[miniBlockSize - 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextLong();
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldWriteNegativeDeltas() throws IOException {
    long[] data = new long[blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = 10 - (i * 32 - random.nextInt(6));
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldWriteAndReadWhenDeltasAreSame() throws IOException {
    long[] data = new long[2 * blockSize];
    for (int i = 0; i < blockSize; i++) {
      data[i] = i * 32;
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldWriteAndReadWhenValuesAreSame() throws IOException {
    long[] data = new long[2 * blockSize];
    for (int i = 0; i < blockSize; i++) {
      data[i] = 3;
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldWriteWhenDeltaIs0ForEachBlock() throws IOException {
    long[] data = new long[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = (i - 1) / blockSize;
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldReadWriteWhenDataIsNotAlignedWithBlock() throws IOException {
    long[] data = new long[5 * blockSize + 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(20) - 10;
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldReadMaxMinValue() throws IOException {
    long[] data = new long[10];
    for (int i = 0; i < data.length; i++) {
      if (i % 2 == 0) {
        data[i] = Long.MIN_VALUE;
      } else {
        data[i] = Long.MAX_VALUE;
      }
    }
    shouldWriteAndRead(data);
  }

  @Test
  public void shouldReturnCorrectOffsetAfterInitialization() throws IOException {
    long[] data = new long[2 * blockSize + 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    writeData(data);

    reader = new DeltaBinaryPackingValuesReader();
    BytesInput bytes = writer.getBytes();
    byte[] valueContent = bytes.toByteArray();
    byte[] pageContent = new byte[valueContent.length * 10];
    int contentOffsetInPage = 33;
    System.arraycopy(valueContent, 0, pageContent, contentOffsetInPage, valueContent.length);

    // offset should be correct
    ByteBufferInputStream stream = ByteBufferInputStream.wrap(ByteBuffer.wrap(pageContent));
    stream.skipFully(contentOffsetInPage);
    reader.initFromPage(100, stream);
    long offset = stream.position();
    assertEquals(valueContent.length + contentOffsetInPage, offset);

    // should be able to read data correctly
    for (long i : data) {
      assertEquals(i, reader.readLong());
    }

    // Testing the deprecated behavior of using byte arrays directly
    reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100, pageContent, contentOffsetInPage);
    assertEquals(valueContent.length + contentOffsetInPage, reader.getNextOffset());
    for (long i : data) {
      assertEquals(i, reader.readLong());
    }
  }

  @Test
  public void shouldThrowExceptionWhenReadMoreThanWritten() throws IOException {
    long[] data = new long[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldWriteAndRead(data);
    try {
      reader.readLong();
    } catch (ParquetDecodingException e) {
      assertEquals("no more value to read, total value count is " + data.length, e.getMessage());
    }
  }

  @Test
  public void shouldSkip() throws IOException {
    long[] data = new long[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    writeData(data);
    reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100, writer.getBytes().toInputStream());
    for (int i = 0; i < data.length; i++) {
      if (i % 3 == 0) {
        reader.skip();
      } else {
        assertEquals(i * 32, reader.readLong());
      }
    }
  }

  @Test
  public void shouldSkipN() throws IOException {
    long[] data = new long[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    writeData(data);
    reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100, writer.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < data.length; i += skipCount + 1) {
      skipCount = (data.length - i) / 2;
      assertEquals(i * 32, reader.readLong());
      reader.skip(skipCount);
    }
  }

  @Test
  public void shouldReset() throws IOException {
    shouldReadWriteWhenDataIsNotAlignedWithBlock();
    long[] data = new long[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i * 2;
    }
    writer.reset();
    shouldWriteAndRead(data);
  }

  @Test
  public void randomDataTest() throws IOException {
    int maxSize = 1000;
    long[] data = new long[maxSize];

    for (int round = 0; round < 100000; round++) {
      int size = random.nextInt(maxSize);

      for (int i = 0; i < size; i++) {
        data[i] = random.nextLong();
      }
      shouldReadAndWrite(data, size);
      writer.reset();
    }
  }

  private void shouldWriteAndRead(long[] data) throws IOException {
    shouldReadAndWrite(data, data.length);
  }

  private void shouldReadAndWrite(long[] data, int length) throws IOException {
    writeData(data, length);
    reader = new DeltaBinaryPackingValuesReader();
    byte[] page = writer.getBytes().toByteArray();
    int miniBlockSize = blockSize / miniBlockNum;

    double miniBlockFlushed = Math.ceil(((double) length - 1) / miniBlockSize);
    double blockFlushed = Math.ceil(((double) length - 1) / blockSize);
    double estimatedSize = 3 * 5 + 1 * 10 //blockHeader, 3 * int + 1 * long
        + 8 * miniBlockFlushed * miniBlockSize //data(aligned to miniBlock)
        + blockFlushed * miniBlockNum //bitWidth of mini blocks
        + (10.0 * blockFlushed);//min delta for each block
    assertTrue(estimatedSize >= page.length);
    reader.initFromPage(100, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    for (int i = 0; i < length; i++) {
      assertEquals(data[i], reader.readLong());
    }
  }

  private void writeData(long[] data) {
    writeData(data, data.length);
  }

  private void writeData(long[] data, int length) {
    for (int i = 0; i < length; i++) {
      writer.writeLong(data[i]);
    }
  }
}
