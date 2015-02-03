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
package parquet.bytes;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Based on DataInputStream but little endian and without the String/char methods
 *
 * @author Julien Le Dem
 *
 */
public final class LittleEndianDataInputStream extends InputStream {

  private final InputStream in;

  /**
   * Creates a LittleEndianDataInputStream that uses the specified
   * underlying InputStream.
   *
   * @param  in   the specified input stream
   */
  public LittleEndianDataInputStream(InputStream in) {
    this.in = in;
  }

  /**
   * See the general contract of the <code>readFully</code>
   * method of <code>DataInput</code>.
   * <p>
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @param      b   the buffer into which the data is read.
   * @exception  EOFException  if this input stream reaches the end before
   *             reading all the bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final void readFully(byte b[]) throws IOException {
    readFully(b, 0, b.length);
  }

  /**
   * See the general contract of the <code>readFully</code>
   * method of <code>DataInput</code>.
   * <p>
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @param      b     the buffer into which the data is read.
   * @param      off   the start offset of the data.
   * @param      len   the number of bytes to read.
   * @exception  EOFException  if this input stream reaches the end before
   *               reading all the bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final void readFully(byte b[], int off, int len) throws IOException {
    if (len < 0)
      throw new IndexOutOfBoundsException();
    int n = 0;
    while (n < len) {
      int count = in.read(b, off + n, len - n);
      if (count < 0)
        throw new EOFException();
      n += count;
    }
  }

  /**
   * See the general contract of the <code>skipBytes</code>
   * method of <code>DataInput</code>.
   * <p>
   * Bytes for this operation are read from the contained
   * input stream.
   *
   * @param      n   the number of bytes to be skipped.
   * @return     the actual number of bytes skipped.
   * @exception  IOException  if the contained input stream does not support
   *             seek, or the stream has been closed and
   *             the contained input stream does not support
   *             reading after close, or another I/O error occurs.
   */
  public final int skipBytes(int n) throws IOException {
    int total = 0;
    int cur = 0;

    while ((total<n) && ((cur = (int) in.skip(n-total)) > 0)) {
      total += cur;
    }

    return total;
  }

  /**
   * @return
   * @throws IOException
   * @see java.io.InputStream#read()
   */
  public int read() throws IOException {
    return in.read();
  }

  /**
   * @return
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return in.hashCode();
  }

  /**
   * @param b
   * @return
   * @throws IOException
   * @see java.io.InputStream#read(byte[])
   */
  public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  /**
   * @param obj
   * @return
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj) {
    return in.equals(obj);
  }

  /**
   * @param b
   * @param off
   * @param len
   * @return
   * @throws IOException
   * @see java.io.InputStream#read(byte[], int, int)
   */
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  /**
   * @param n
   * @return
   * @throws IOException
   * @see java.io.InputStream#skip(long)
   */
  public long skip(long n) throws IOException {
    return in.skip(n);
  }

  /**
   * @return
   * @throws IOException
   * @see java.io.InputStream#available()
   */
  public int available() throws IOException {
    return in.available();
  }

  /**
   * @throws IOException
   * @see java.io.InputStream#close()
   */
  public void close() throws IOException {
    in.close();
  }

  /**
   * @param readlimit
   * @see java.io.InputStream#mark(int)
   */
  public void mark(int readlimit) {
    in.mark(readlimit);
  }

  /**
   * @throws IOException
   * @see java.io.InputStream#reset()
   */
  public void reset() throws IOException {
    in.reset();
  }

  /**
   * @return
   * @see java.io.InputStream#markSupported()
   */
  public boolean markSupported() {
    return in.markSupported();
  }

  /**
   * See the general contract of the <code>readBoolean</code>
   * method of <code>DataInput</code>.
   * <p>
   * Bytes for this operation are read from the contained
   * input stream.
   *
   * @return     the <code>boolean</code> value read.
   * @exception  EOFException  if this input stream has reached the end.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final boolean readBoolean() throws IOException {
    int ch = in.read();
    if (ch < 0)
      throw new EOFException();
    return (ch != 0);
  }

  /**
   * See the general contract of the <code>readByte</code>
   * method of <code>DataInput</code>.
   * <p>
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next byte of this input stream as a signed 8-bit
   *             <code>byte</code>.
   * @exception  EOFException  if this input stream has reached the end.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final byte readByte() throws IOException {
    int ch = in.read();
    if (ch < 0)
      throw new EOFException();
    return (byte)(ch);
  }

  /**
   * See the general contract of the <code>readUnsignedByte</code>
   * method of <code>DataInput</code>.
   * <p>
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next byte of this input stream, interpreted as an
   *             unsigned 8-bit number.
   * @exception  EOFException  if this input stream has reached the end.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see         java.io.FilterInputStream#in
   */
  public final int readUnsignedByte() throws IOException {
    int ch = in.read();
    if (ch < 0)
      throw new EOFException();
    return ch;
  }

  /**
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next two bytes of this input stream, interpreted as a
   *             signed 16-bit number.
   * @exception  EOFException  if this input stream reaches the end before
   *               reading two bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final short readShort() throws IOException {
    int ch2 = in.read();
    int ch1 = in.read();
    if ((ch1 | ch2) < 0)
      throw new EOFException();
    return (short)((ch1 << 8) + (ch2 << 0));
  }

  /**
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next two bytes of this input stream, interpreted as an
   *             unsigned 16-bit integer.
   * @exception  EOFException  if this input stream reaches the end before
   *             reading two bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final int readUnsignedShort() throws IOException {
    int ch2 = in.read();
    int ch1 = in.read();
    if ((ch1 | ch2) < 0)
      throw new EOFException();
    return (ch1 << 8) + (ch2 << 0);
  }

  /**
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next four bytes of this input stream, interpreted as an
   *             <code>int</code>.
   * @exception  EOFException  if this input stream reaches the end before
   *               reading four bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final int readInt() throws IOException {
    // TODO: has this been benchmarked against two alternate implementations?
    // 1) Integer.reverseBytes(in.readInt())
    // 2) keep a member byte[4], wrapped by an IntBuffer with appropriate endianness set,
    //    and call IntBuffer.get()
    // Both seem like they might be faster.
    int ch4 = in.read();
    int ch3 = in.read();
    int ch2 = in.read();
    int ch1 = in.read();
    if ((ch1 | ch2 | ch3 | ch4) < 0)
      throw new EOFException();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  private byte readBuffer[] = new byte[8];

  /**
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next eight bytes of this input stream, interpreted as a
   *             <code>long</code>.
   * @exception  EOFException  if this input stream reaches the end before
   *               reading eight bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.io.FilterInputStream#in
   */
  public final long readLong() throws IOException {
    // TODO: see perf question above in readInt
    readFully(readBuffer, 0, 8);
    return (((long)readBuffer[7] << 56) +
        ((long)(readBuffer[6] & 255) << 48) +
        ((long)(readBuffer[5] & 255) << 40) +
        ((long)(readBuffer[4] & 255) << 32) +
        ((long)(readBuffer[3] & 255) << 24) +
        ((readBuffer[2] & 255) << 16) +
        ((readBuffer[1] & 255) <<  8) +
        ((readBuffer[0] & 255) <<  0));
  }

  /**
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next four bytes of this input stream, interpreted as a
   *             <code>float</code>.
   * @exception  EOFException  if this input stream reaches the end before
   *               reading four bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.lang.Float#intBitsToFloat(int)
   */
  public final float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  /**
   * Bytes
   * for this operation are read from the contained
   * input stream.
   *
   * @return     the next eight bytes of this input stream, interpreted as a
   *             <code>double</code>.
   * @exception  EOFException  if this input stream reaches the end before
   *               reading eight bytes.
   * @exception  IOException   the stream has been closed and the contained
   *             input stream does not support reading after close, or
   *             another I/O error occurs.
   * @see        java.lang.Double#longBitsToDouble(long)
   */
  public final double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

}
