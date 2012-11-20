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
import java.io.IOException;

public class BitWriter {
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
    currentByte = setBytePosition(currentByte, currentBytePosition++, bit);
    if (currentBytePosition == 8) {
      baos.write(currentByte);
      currentByte = 0;
      currentBytePosition = 0;
    }
  }

  public void writeByte(int val) {
    currentByte |= ((val & 0xFF) << currentBytePosition);
    baos.write(currentByte);
    currentByte >>>= 8;
  }

  public void writeBits(int val, int bitsToWrite) {
    val <<= currentBytePosition;
    int upperByte = currentBytePosition + bitsToWrite;
    currentByte |= val;
    while (upperByte >= 8) {
      baos.write(currentByte); //this only writes the lowest byte
      upperByte -= 8;
      currentByte >>>= 8;
    }
    currentBytePosition = (currentBytePosition + bitsToWrite) % 8;
  }

  public byte[] finish() {
    if (!finished) {
      if (currentBytePosition > 0) {
        baos.write(currentByte);
      }
      try {
        baos.flush();
      } catch (IOException e) {
        // This shouldn't be possible as ByteArrayOutputStream
        // uses OutputStream's flush, which is a noop
        throw new RuntimeException(e);
      }
    }
    byte[] buf = baos.toByteArray();
    finished = true;
    return buf;
  }

  public void reset() {
    baos = new ByteArrayOutputStream();
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