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
package org.apache.parquet.column.values.symboltable;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.SymbolTablePage;
import org.apache.parquet.column.values.RequiresFallback;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.io.api.Binary;

/**
 * Writes binary values as codes over a symbol table trained on the data.
 *
 * <p>One writer serves every symbol table representation. The representation decides how a table is
 * trained, how it is serialized and how a code stream is framed, and all three sit behind
 * {@link SymbolTableTrainer}, {@link SymbolTable} and {@link CodeStreamEncoder}; nothing about them
 * reaches this class beyond the {@link SymbolTableType} it is asked for.
 *
 * <h2>What happens when</h2>
 *
 * <p>Values are buffered raw as they arrive. The table is trained at the first {@link #getBytes()},
 * on that page's values, and kept for the rest of the chunk: a table belongs to a column chunk, so
 * a later page must not train its own. It is handed off through {@link #toSymbolTablePageAndClose()},
 * called once per chunk after every page has been written, which is also what keeps a chunk that
 * falls back to another encoding from leaving a table behind that no page refers to. Training on
 * the first page rather than on the whole chunk is deliberate — a page is already far more text
 * than a trainer samples, and buffering the chunk to feed it would cost a second copy of the
 * column.
 *
 * <p>Buffering is the reason values are held at all: a trainer reads them in an order of its own
 * choosing and more than once, and a fallback to another encoding has to replay them. The cost is
 * the page held twice, once raw and once as codes, which is what {@link #getAllocatedSize()}
 * reports.
 *
 * <h2>Falling back</h2>
 *
 * <p>Compressing a page can make it bigger — short values, or values that share nothing with the
 * rest of the column. Wrapping this writer in a
 * {@link org.apache.parquet.column.values.fallback.FallbackValuesWriter} over a plain writer is what
 * makes the encoding safe to turn on: {@link #isCompressionSatisfying} answers the only question
 * that matters, which is whether the codes came out smaller than the values.
 */
public class SymbolTableValuesWriter extends ValuesWriter implements RequiresFallback {

  private final SymbolTableType type;
  private final ValueBuffer values;
  private final SymbolTablePayloadWriter payload;

  /** The chunk's table, trained at the first {@link #getBytes()} and reused after that. */
  private TrainedSymbolTable trained;

  /** Scratch for one value's codes, grown as needed and reused across values. */
  private byte[] codes = new byte[0];

  /**
   * How many of the buffered values have been compressed into {@link #payload}.
   *
   * <p>Compression happens at {@link #getBytes()}, because it cannot start before the table exists.
   * Counting what is already done rather than flagging it keeps a second call correct whether or not
   * more values arrived in between.
   */
  private int compressedCount;

  public SymbolTableValuesWriter(
      SymbolTableType type,
      OffsetEncoding offsetEncoding,
      int initialSlabSize,
      int pageSize,
      ByteBufferAllocator allocator) {
    this.type = type;
    this.values = new ValueBuffer();
    this.payload = new SymbolTablePayloadWriter(offsetEncoding, initialSlabSize, pageSize, allocator);
  }

  public SymbolTableType symbolTableType() {
    return type;
  }

  @Override
  public void writeBytes(Binary v) {
    values.add(v);
  }

  /**
   * The size of the values buffered for this page, counted as a plain page would count them.
   *
   * <p>Not the compressed size, which is not known until the table has been trained. A page boundary
   * has to be decided while values are still arriving, so it is decided on what the page would cost
   * unencoded — the same choice {@link org.apache.parquet.column.values.fallback.FallbackValuesWriter}
   * makes, and for the same reason: a page sized by its compressed length is a page that becomes too
   * big the moment the encoding is abandoned.
   */
  @Override
  public long getBufferedSize() {
    return values.byteCount() + 4L * values.valueCount();
  }

  @Override
  public BytesInput getBytes() {
    if (trained == null) {
      trained = SymbolTables.trainer(type).train(values);
    }
    compressBufferedValues();
    return payload.getBytes();
  }

  @Override
  public SymbolTablePage toSymbolTablePageAndClose() {
    return trained == null ? null : new SymbolTablePage(trained.table().serialize(), type);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.FSST;
  }

  @Override
  public void reset() {
    values.reset();
    payload.reset();
    compressedCount = 0;
  }

  /**
   * Drops the table, which is what a new column chunk needs.
   *
   * <p>Named for the dictionary because that is the only chunk-scoped state a values writer had
   * before this one. In the file writer a values writer does not outlive its chunk, so this is
   * belt-and-braces rather than the path that runs.
   */
  @Override
  public void resetDictionary() {
    trained = null;
  }

  @Override
  public void close() {
    payload.close();
  }

  @Override
  public long getAllocatedSize() {
    return values.allocatedSize() + payload.allocatedSize() + codes.length;
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s %s{raw %d bytes, codes %d bytes}", prefix, type, values.byteCount(), payload.bufferedSize());
  }

  // RequiresFallback

  /**
   * Never, because there is no state here that can run away.
   *
   * <p>This is the check a dictionary needs, where the encoding stops paying once the dictionary
   * outgrows the data. A symbol table is bounded by its representation whatever the data does, so
   * the only question worth asking is the one {@link #isCompressionSatisfying} asks, once, when
   * there is an answer to it.
   */
  @Override
  public boolean shouldFallBack() {
    return false;
  }

  @Override
  public boolean isCompressionSatisfying(long rawSize, long encodedSize) {
    return encodedSize < rawSize;
  }

  @Override
  public void fallBackAllValuesTo(ValuesWriter writer) {
    byte[] data = values.data();
    for (int i = 0; i < values.valueCount(); i++) {
      writer.writeBytes(Binary.fromReusedByteArray(data, values.offset(i), values.length(i)));
    }
  }

  private void compressBufferedValues() {
    CodeStreamEncoder encoder = trained.encoder();
    byte[] data = values.data();
    for (int i = compressedCount; i < values.valueCount(); i++) {
      int length = values.length(i);
      int bound = encoder.maxCompressedLength(length);
      if (codes.length < bound) {
        codes = new byte[Math.max(bound, codes.length * 2)];
      }
      payload.addValue(codes, 0, encoder.compress(data, values.offset(i), length, codes, 0));
    }
    compressedCount = values.valueCount();
  }
}
