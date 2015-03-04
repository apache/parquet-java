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
import java.io.OutputStream;
import java.nio.charset.Charset;

import parquet.Log;

/**
 * utility methods to deal with bytes
 *
 * @author Julien Le Dem
 *
 */
public class BytesUtils {
  private static final Log LOG = Log.getLog(BytesUtils.class);

  public static final Charset UTF8 = Charset.forName("UTF-8");

  /**
   * give the number of bits needed to encode an int given the max value
   * @param bound max int that we want to encode
   * @return the number of bits required
   */
  public static int getWidthFromMaxInt(int bound) {
    return 32 - Integer.numberOfLeadingZeros(bound);
  }

  /**
   * reads an int in little endian at the given position
   * @param in
   * @param offset
   * @return
   * @throws IOException
   */
  public static int readIntLittleEndian(byte[] in, int offset) throws IOException {
    int ch4 = in[offset] & 0xff;
    int ch3 = in[offset + 1] & 0xff;
    int ch2 = in[offset + 2] & 0xff;
    int ch1 = in[offset + 3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static int readIntLittleEndian(InputStream in) throws IOException {
    // TODO: this is duplicated code in LittleEndianDataInputStream
    int ch1 = in.read();
    int ch2 = in.read();
    int ch3 = in.read();
    int ch4 = in.read();
    if ((ch1 | ch2 | ch3 | ch4) < 0) {
        throw new EOFException();
    }
    return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
  }

  public static int readIntLittleEndianOnOneByte(InputStream in) throws IOException {
      int ch1 = in.read();
      if (ch1 < 0) {
        throw new EOFException();
      }
      return ch1;
  }

  public static int readIntLittleEndianOnTwoBytes(InputStream in) throws IOException {
      int ch1 = in.read();
      int ch2 = in.read();
      if ((ch1 | ch2 ) < 0) {
          throw new EOFException();
      }
      return ((ch2 << 8) + (ch1 << 0));
  }

  public static int readIntLittleEndianOnThreeBytes(InputStream in) throws IOException {
      int ch1 = in.read();
      int ch2 = in.read();
      int ch3 = in.read();
      if ((ch1 | ch2 | ch3 ) < 0) {
          throw new EOFException();
      }
      return ((ch3 << 16) + (ch2 << 8) + (ch1 << 0));
  }

  public static int readIntLittleEndianPaddedOnBitWidth(InputStream in, int bitWidth)
      throws IOException {

    int bytesWidth = paddedByteCountFromBits(bitWidth);
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return BytesUtils.readIntLittleEndianOnOneByte(in);
      case 2:
        return BytesUtils.readIntLittleEndianOnTwoBytes(in);
      case 3:
        return  BytesUtils.readIntLittleEndianOnThreeBytes(in);
      case 4:
        return BytesUtils.readIntLittleEndian(in);
      default:
        throw new IOException(
          String.format("Encountered bitWidth (%d) that requires more than 4 bytes", bitWidth));
    }
  }

  public static void writeIntLittleEndianOnOneByte(OutputStream out, int v) throws IOException {
    out.write((v >>>  0) & 0xFF);
  }

  public static void writeIntLittleEndianOnTwoBytes(OutputStream out, int v) throws IOException {
    out.write((v >>>  0) & 0xFF);
    out.write((v >>>  8) & 0xFF);
  }

  public static void writeIntLittleEndianOnThreeBytes(OutputStream out, int v) throws IOException {
    out.write((v >>>  0) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>> 16) & 0xFF);
  }

  public static void writeIntLittleEndian(OutputStream out, int v) throws IOException {
    // TODO: this is duplicated code in LittleEndianDataOutputStream
    out.write((v >>>  0) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 24) & 0xFF);
    if (Log.DEBUG) LOG.debug("write le int: " + v + " => "+ ((v >>>  0) & 0xFF) + " " + ((v >>>  8) & 0xFF) + " " + ((v >>> 16) & 0xFF) + " " + ((v >>> 24) & 0xFF));
  }

  /**
   * Write a little endian int to out, using the the number of bytes required by
   * bit width
   */
  public static void writeIntLittleEndianPaddedOnBitWidth(OutputStream out, int v, int bitWidth)
      throws IOException {

    int bytesWidth = paddedByteCountFromBits(bitWidth);
    switch (bytesWidth) {
      case 0:
        break;
      case 1:
        writeIntLittleEndianOnOneByte(out, v);
        break;
      case 2:
        writeIntLittleEndianOnTwoBytes(out, v);
        break;
      case 3:
        writeIntLittleEndianOnThreeBytes(out, v);
        break;
      case 4:
        writeIntLittleEndian(out, v);
        break;
      default:
        throw new IOException(
          String.format("Encountered value (%d) that requires more than 4 bytes", v));
    }
  }

  public static int readUnsignedVarInt(InputStream in) throws IOException {
    int value = 0;
    int i = 0;
    int b;
    while (((b = in.read()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | (b << i);
  }

  /**
   * uses a trick mentioned in https://developers.google.com/protocol-buffers/docs/encoding to read zigZag encoded data
   * @param in
   * @return
   * @throws IOException
   */
  public static int readZigZagVarInt(InputStream in) throws IOException {
    int raw = readUnsignedVarInt(in);
    int temp = (((raw << 31) >> 31) ^ raw) >> 1;
    return temp ^ (raw & (1 << 31));
  }

  public static void writeUnsignedVarInt(int value, OutputStream out) throws IOException {
    while ((value & 0xFFFFFF80) != 0L) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value & 0x7F);
  }

  public static void writeZigZagVarInt(int intValue, OutputStream out) throws IOException{
    writeUnsignedVarInt((intValue << 1) ^ (intValue >> 31), out);
  }

  /**
   * @param bitLength a count of bits
   * @return the corresponding byte count padded to the next byte
   */
  public static int paddedByteCountFromBits(int bitLength) {
    return (bitLength + 7) / 8;
  }

  public static byte[] intToBytes(int value) {
    byte[] outBuffer = new byte[4];
    outBuffer[3] = (byte)(value >>> 24);
    outBuffer[2] = (byte)(value >>> 16);
    outBuffer[1] = (byte)(value >>>  8);
    outBuffer[0] = (byte)(value >>>  0);
    return outBuffer;
  }

  public static int bytesToInt(byte[] bytes) {
    return ((int)(bytes[3] & 255) << 24) +
           ((int)(bytes[2] & 255) << 16) +
           ((int)(bytes[1] & 255) <<  8) +
           ((int)(bytes[0] & 255) <<  0);
  }

  public static byte[] longToBytes(long value) {
    byte[] outBuffer = new byte[8];
    outBuffer[7] = (byte)(value >>> 56);
    outBuffer[6] = (byte)(value >>> 48);
    outBuffer[5] = (byte)(value >>> 40);
    outBuffer[4] = (byte)(value >>> 32);
    outBuffer[3] = (byte)(value >>> 24);
    outBuffer[2] = (byte)(value >>> 16);
    outBuffer[1] = (byte)(value >>>  8);
    outBuffer[0] = (byte)(value >>>  0);
    return outBuffer;
  }

  public static long bytesToLong(byte[] bytes) {
    return (((long)bytes[7] << 56) +
           ((long)(bytes[6] & 255) << 48) +
           ((long)(bytes[5] & 255) << 40) +
           ((long)(bytes[4] & 255) << 32) +
           ((long)(bytes[3] & 255) << 24) +
           ((long)(bytes[2] & 255) << 16) +
           ((long)(bytes[1] & 255) <<  8) +
           ((long)(bytes[0] & 255) <<  0));
  }

  public static byte[] booleanToBytes(boolean value) {
    byte[] outBuffer = new byte[1];
    outBuffer[0] = (byte)(value ? 1 : 0);
    return outBuffer;
  }

  public static boolean bytesToBool(byte[] bytes) {
    return ((int)(bytes[0] & 255) != 0);
  }
}
