/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.column.primitive;

import java.io.ByteArrayOutputStream;

import redelm.Log;

public class BitWriter {
  private static final Log LOG = Log.getLog(BitWriter.class);
  private static final boolean DEBUG = false;//Log.DEBUG;

  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private int currentByte = 0;
  private int currentBytePosition = 0;
  private static final int[] byteToTrueMask = new int[8];
  private static final int[] byteToFalseMask = new int[8];
  private boolean finished = false;
  static {
    int currentMask = 1;
    for (int i = 0; i < byteToTrueMask.length; i++) {
      byteToTrueMask[i] = currentMask;
      byteToFalseMask[i] = ~currentMask;
      currentMask <<= 1;
    }
  }

  public void writeBit(boolean bit) {
    if (DEBUG) LOG.debug("writing: " + (bit ? "1" : "0"));
    currentByte = setBytePosition(currentByte, currentBytePosition++, bit);
    if (currentBytePosition == 8) {
      baos.write(currentByte);
      if (DEBUG) LOG.debug("to buffer: " + toBinary(currentByte));
      currentByte = 0;
      currentBytePosition = 0;
    }
  }

  public void writeByte(int val) {
    if (DEBUG) LOG.debug("writing: " + toBinary(val) + " (" + val + ")");
    currentByte |= ((val & 0xFF) << currentBytePosition);
    baos.write(currentByte);
    if (DEBUG) LOG.debug("to buffer: " + toBinary(currentByte));
    currentByte >>>= 8;
  }

  public void writeBits(int val, int bitsToWrite) {
    if (DEBUG) LOG.debug("writing: " + toBinary(val, bitsToWrite) + " (" + val + ")");
    val <<= currentBytePosition;
    int upperByte = currentBytePosition + bitsToWrite;
    currentByte |= val;
    while (upperByte >= 8) {
      baos.write(currentByte); //this only writes the lowest byte
      if (DEBUG) LOG.debug("to buffer: " + toBinary(currentByte));
      upperByte -= 8;
      currentByte >>>= 8;
    }
    currentBytePosition = (currentBytePosition + bitsToWrite) % 8;
  }

  private String toBinary(int val, int alignTo) {
    String result = Integer.toBinaryString(val);
    while (result.length() < alignTo) {
      result = "0" + result;
    }
    return result;
  }

  private String toBinary(int val) {
    return toBinary(val, 8);
  }

  public byte[] finish() {
    if (!finished) {
      if (currentBytePosition > 0) {
        baos.write(currentByte);
        if (DEBUG) LOG.debug("to buffer: " + toBinary(currentByte));
      }
    }
    byte[] buf = baos.toByteArray();
    finished = true;
    return buf;
  }

  public void reset() {
    baos.reset();
    currentByte = 0;
    currentBytePosition = 0;
    finished = false;
  }

  public static int setBytePosition(int currentByte, int currentBytePosition, boolean bit) {
    if (bit) {
      currentByte |= byteToTrueMask[currentBytePosition];
    } else {
      currentByte &= byteToFalseMask[currentBytePosition];
    }
    return currentByte;
  }

  //This assumes you will never give it a negative value
  public void writeUnsignedVarint(int value) {
    while ((value & 0xFFFFFF80) != 0L) {
      writeByte((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    writeByte(value & 0x7F);
  }

  public int getMemSize() {
    // baos = 8 bytes
    // currentByte + currentBytePosition = 8 bytes
    // the size of baos:
    //   count : 4 bytes (rounded to 8)
    //   buf : 12 bytes (8 ptr + 4 length) should technically be rounded to 8 depending on buffer size
    return 32 + baos.size() * 8;
  }
}