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
package org.apache.parquet.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@code DiskOutputFile} is an implementation needed by Parquet to write
 * data files to disk using {@link PositionOutputStream} instances.
 */
public class DiskOutputFile implements OutputFile {

  private final Path path;

  public DiskOutputFile(Path file) {
    path = file;
  }

  /**
   * Opens a new {@link PositionOutputStream} for the data file to create.
   *
   * @return a new {@link PositionOutputStream} to write the file
   * @throws IOException if the stream cannot be opened
   */
  @Override
  public PositionOutputStream create(long buffer) throws IOException {
    return new PositionOutputStream() {

      private final BufferedOutputStream stream =
        new BufferedOutputStream(Files.newOutputStream(path), (int) buffer);
      private long pos = 0;

      /**
       * Reports the current position of this output stream.
       *
       * @return a long, the current position in bytes starting from 0
       */
      @Override
      public long getPos() {
        return pos;
      }

      /**
       * Writes the specified byte to this output stream. The general
       * contract for {@code write} is that one byte is written
       * to the output stream. The byte to be written is the eight
       * low-order bits of the argument {@code b}. The 24
       * high-order bits of {@code b} are ignored.
       *
       * @param      data the {@code byte}.
       * @throws     IOException  if an I/O error occurs. In particular,
       *             an {@code IOException} may be thrown if the
       *             output stream has been closed.
       */
      @Override
      public void write(int data) throws IOException {
        pos++;
        stream.write(data);
      }

      /**
       * Writes {@code data.length} bytes from the specified byte array
       * to this output stream. The general contract for {@code write(data)}
       * is that it should have exactly the same effect as the call
       * {@code write(data, 0, data.length)}.
       *
       * @param      data the data.
       * @throws     IOException  if an I/O error occurs.
       * @see        java.io.OutputStream#write(byte[], int, int)
       */
      @Override
      public void write(byte[] data) throws IOException {
        pos += data.length;
        stream.write(data);
      }

      /**
       * Writes {@code len} bytes from the specified byte array
       * starting at offset {@code off} to this output stream.
       * The general contract for {@code write(b, off, len)} is that
       * some of the bytes in the array {@code b} are written to the
       * output stream in order; element {@code b[off]} is the first
       * byte written and {@code b[off+len-1]} is the last byte written
       * by this operation.
       * <p>
       * The {@code write} method of {@code OutputStream} calls
       * the write method of one argument on each of the bytes to be
       * written out. Subclasses are encouraged to override this method and
       * provide a more efficient implementation.
       * <p>
       * If {@code b} is {@code null}, a
       * {@code NullPointerException} is thrown.
       * <p>
       * If {@code off} is negative, or {@code len} is negative, or
       * {@code off+len} is greater than the length of the array
       * {@code b}, then an {@code IndexOutOfBoundsException} is thrown.
       *
       * @param      data  the data.
       * @param      off   the start offset in the data.
       * @param      len   the number of bytes to write.
       * @throws     IOException  if an I/O error occurs. In particular,
       *             an {@code IOException} is thrown if the output
       *             stream is closed.
       */
      @Override
      public void write(byte[] data, int off, int len) throws IOException {
        pos += len;
        stream.write(data, off, len);
      }

      /**
       * Flushes this output stream and forces any buffered output bytes
       * to be written out. The general contract of {@code flush} is
       * that calling it is an indication that, if any bytes previously
       * written have been buffered by the implementation of the output
       * stream, such bytes should immediately be written to their
       * intended destination.
       * <p>
       * If the intended destination of this stream is an abstraction provided by
       * the underlying operating system, for example a file, then flushing the
       * stream guarantees only that bytes previously written to the stream are
       * passed to the operating system for writing; it does not guarantee that
       * they are actually written to a physical device such as a disk drive.
       *
       * @throws     IOException  if an I/O error occurs.
       */
      @Override
      public void flush() throws IOException {
        stream.flush();
      }

      /**
       * Closes this output stream and releases any system resources
       * associated with this stream. The general contract of {@code close}
       * is that it closes the output stream. A closed stream cannot perform
       * output operations and cannot be reopened.
       *
       * @throws     IOException  if an I/O error occurs.
       */
      @Override
      public void close() throws IOException {
        stream.close();
      }
    };
  }

  /**
   * Opens a new {@link PositionOutputStream} for the data file to create or overwrite.
   *
   * @return a new {@link PositionOutputStream} to write the file
   * @throws IOException if the stream cannot be opened
   */
  @Override
  public PositionOutputStream createOrOverwrite(long buffer) throws IOException {
    return create(buffer);
  }

  /**
   * @return a flag indicating if block size is supported.
   */
  @Override
  public boolean supportsBlockSize() {
    return true;
  }

  /**
   * @return the default block size.
   */
  @Override
  public long defaultBlockSize() {
    return 512;
  }

  /**
   * @return the path of the file, as a {@link String}.
   */
  @Override
  public String getPath() {
    return path.toString();
  }
}
