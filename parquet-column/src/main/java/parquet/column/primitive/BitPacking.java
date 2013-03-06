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
package parquet.column.primitive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import parquet.column.primitive.BitPacking.BitPackingReader;
import parquet.column.primitive.BitPacking.BitPackingWriter;

// TODO: rework the whole thing. It does not need to use streams at all
/**
 * provides the correct implementation of a bitpacking based on the width in bits
 *
 * @author Julien Le Dem
 *
 */
public class BitPacking {

  /**
   * to writes ints to a stream packed to only the needed bits.
   * there is no guarantee of corecteness if ints larger than the max size are written
   *
   * @author Julien Le Dem
   *
   */
  abstract public static class BitPackingWriter {
    /**
     * will write the bits to the underlying stream aligned on the buffer size
     * @param val the value to encode
     * @throws IOException
     */
    abstract public void write(int val) throws IOException;

    /**
     * will flush the buffer to the underlying stream (and pad with 0s)
     * @throws IOException
     */
    abstract public void finish() throws IOException;
  }

  /**
   * to read back what has been written with the corresponding  writer
   *
   * @author Julien Le Dem
   *
   */
  abstract public static class BitPackingReader {

    /**
     *
     * @return and int decoded from the underlying stream
     * @throws IOException
     */
    abstract public int read() throws IOException;
  }

  private BitPacking() {
  }

  /**
   * @param bitLength the width in bits of the integers to write
   * @param out the stream to write the bytes to
   * @return the correct implementation for the width
   */
  public static BitPackingWriter getBitPackingWriter(int bitLength, OutputStream out) {
    switch (bitLength) {
    case 0:
      return new ZeroBitPackingWriter();
    case 1:
      return new OneBitPackingWriter(out);
    case 2:
      return new TwoBitPackingWriter(out);
    case 3:
      return new ThreeBitPackingWriter(out);
    case 4:
      return new FourBitPackingWriter(out);
    case 5:
      return new FiveBitPackingWriter(out);
    case 6:
      return new SixBitPackingWriter(out);
    case 7:
      return new SevenBitPackingWriter(out);
    case 8:
      return new EightBitPackingWriter(out);
    default:
      throw new UnsupportedOperationException("only support up to 8 for now");
    }
  }

  /**
   *
   * @param bitLength the width in bits of the integers to read
   * @param inthe stream to read the bytes from
   * @return the correct implementation for the width
   */
  public static BitPackingReader createBitPackingReader(int bitLength, InputStream in, long valueCount) {
    switch (bitLength) {
    case 0:
      return new ZeroBitPackingReader();
    case 1:
      return new OneBitPackingReader(in);
    case 2:
      return new TwoBitPackingReader(in);
    case 3:
      return new ThreeBitPackingReader(in, valueCount);
    case 4:
      return new FourBitPackingReader(in);
    case 5:
      return new FiveBitPackingReader(in, valueCount);
    case 6:
      return new SixBitPackingReader(in, valueCount);
    case 7:
      return new SevenBitPackingReader(in, valueCount);
    case 8:
      return new EightBitPackingReader(in);
    default:
      throw new UnsupportedOperationException("only support up to 8 for now");
    }
  }
}

abstract class BaseBitPackingWriter extends BitPackingWriter {

  void finish(int numberOfBits, int buffer, OutputStream out) throws IOException {
    int padding = numberOfBits % 8 == 0 ? 0 : 8 - (numberOfBits % 8);
    buffer = buffer << padding;
    int numberOfBytes = (numberOfBits + padding) / 8;
    for (int i = (numberOfBytes - 1) * 8; i >= 0 ; i -= 8) {
      out.write((buffer >>> i) & 0xFF);
    }
  }

  void finish(int numberOfBits, long buffer, OutputStream out) throws IOException {
    int padding = numberOfBits % 8 == 0 ? 0 : 8 - (numberOfBits % 8);
    buffer = buffer << padding;
    int numberOfBytes = (numberOfBits + padding) / 8;
    for (int i = (numberOfBytes - 1) * 8; i >= 0 ; i -= 8) {
      out.write((int)(buffer >>> i) & 0xFF);
    }
  }
}
abstract class BaseBitPackingReader extends BitPackingReader {

  int alignToBytes(int bitsCount) {
    return bitsCount / 8 + (bitsCount % 8 == 0 ? 0 : 1);
  }

}

class ZeroBitPackingWriter extends BitPackingWriter {

  @Override
  public void write(int val) throws IOException {
  }

  @Override
  public void finish() {
  }

}
class ZeroBitPackingReader extends BitPackingReader {

  @Override
  public int read() throws IOException {
    return 0;
  }

}

class OneBitPackingWriter extends BitPackingWriter {

  private OutputStream out;

  private int buffer = 0;
  private int count = 0;

  public OneBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 1;
    buffer |= val;
    ++ count;
    if (count == 8) {
      out.write(buffer);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    while (count != 0) {
      write(0);
    }
    // check this does not impede perf
    out = null;
  }

}
class OneBitPackingReader extends BitPackingReader {

  private final InputStream in;

  private int buffer = 0;
  private int count = 0;

  public OneBitPackingReader(InputStream in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      buffer = in.read();
      count = 8;
    }
    int result = (buffer >> (count - 1)) & 1;
    -- count;
    return result;
  }

}

class TwoBitPackingWriter extends BitPackingWriter {

  private OutputStream out;

  private int buffer = 0;
  private int count = 0;

  public TwoBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 2;
    buffer |= val;
    ++ count;
    if (count == 4) {
      out.write(buffer);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    while (count != 0) {
      write(0);
    }
    // check this does not impede perf
    out = null;
  }

}
class TwoBitPackingReader extends BitPackingReader {

  private final InputStream in;

  private int buffer = 0;
  private int count = 0;

  public TwoBitPackingReader(InputStream in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      buffer = in.read();
      count = 4;
    }
    int result = (buffer >> ((count - 1) * 2)) & 3;
    -- count;
    return result;
  }

}

class ThreeBitPackingWriter extends BaseBitPackingWriter {

  private OutputStream out;

  private int buffer = 0;
  private int count = 0;

  public ThreeBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 3;
    buffer |= val;
    ++ count;
    if (count == 8) {
      out.write((buffer >>> 16) & 0xFF);
      out.write((buffer >>>  8) & 0xFF);
      out.write((buffer >>>  0) & 0xFF);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    if (count != 0) {
      int numberOfBits = count * 3;
      finish(numberOfBits, buffer, out);
      buffer = 0;
      count = 0;
    }
    // check this does not impede perf
    out = null;
  }

}
class ThreeBitPackingReader extends BaseBitPackingReader {

  private final InputStream in;
  private final long valueCount;

  private int buffer = 0;
  private int count = 0;

  private long totalRead = 0;

  public ThreeBitPackingReader(InputStream in, long valueCount) {
    this.in = in;
    this.valueCount = valueCount;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      if (valueCount - totalRead < 8) {
        buffer = 0;
        int bitsToRead = 3 * (int)(valueCount - totalRead);
        int bytesToRead = alignToBytes(bitsToRead);
        for (int i = 3 - 1 ; i >= 3 - bytesToRead ; i--) {
          buffer |= in.read() << (i * 8);
        }
        count = 8;
        totalRead = valueCount;
      } else {
        buffer = (in.read() << 16) + (in.read() << 8) + in.read();
        count = 8;
        totalRead += 8;
      }
    }
    int result = (buffer >> ((count - 1) * 3)) & 7;
    -- count;
    return result;
  }

}

class FourBitPackingWriter extends BitPackingWriter {

  private OutputStream out;

  private int buffer = 0;
  private int count = 0;

  public FourBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 4;
    buffer |= val;
    ++ count;
    if (count == 2) {
      out.write(buffer);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    while (count != 0) {
      // downside: this aligns on whatever the buffer size is.
      write(0);
    }
    // check this does not impede perf
    out = null;
  }

}
class FourBitPackingReader extends BitPackingReader {

  private final InputStream in;

  private int buffer = 0;
  private int count = 0;

  public FourBitPackingReader(InputStream in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      buffer = in.read();
      count = 2;
    }
    int result = (buffer >> ((count - 1) * 4)) & 15;
    -- count;
    return result;
  }

}

class FiveBitPackingWriter extends BaseBitPackingWriter {

  private OutputStream out;

  private long buffer = 0;
  private int count = 0;

  public FiveBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 5;
    buffer |= val;
    ++ count;
    if (count == 8) {
      out.write((int)(buffer >>> 32) & 0xFF);
      out.write((int)(buffer >>> 24) & 0xFF);
      out.write((int)(buffer >>> 16) & 0xFF);
      out.write((int)(buffer >>>  8) & 0xFF);
      out.write((int)(buffer >>>  0) & 0xFF);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    if (count != 0) {
      int numberOfBits = count * 5;
      finish(numberOfBits, buffer, out);
      buffer = 0;
      count = 0;
    }
    // check this does not impede perf
    out = null;
  }

}
class FiveBitPackingReader extends BaseBitPackingReader {

  private final InputStream in;
  private final long valueCount;

  private long buffer = 0;
  private int count = 0;
  private long totalRead = 0;


  public FiveBitPackingReader(InputStream in, long valueCount) {
    this.in = in;
    this.valueCount = valueCount;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      if (valueCount - totalRead < 8) {
        buffer = 0;
        int bitsToRead = 5 * (int)(valueCount - totalRead);
        int bytesToRead = alignToBytes(bitsToRead);
        for (int i = 5 - 1; i >= 5 - bytesToRead ; i--) {
          buffer |= (((long)in.read()) & 255) << (i * 8);
        }
        count = 8;
        totalRead = valueCount;
      } else {
        buffer =
            ((((long)in.read()) & 255) << 32)
            + ((((long)in.read()) & 255) << 24)
            + (in.read() << 16)
            + (in.read() << 8)
            + in.read();
        count = 8;
        totalRead += 8;
      }
    }
    int result = (((int)(buffer >> ((count - 1) * 5))) & 31);
    -- count;
    return result;
  }

}

class SixBitPackingWriter extends BaseBitPackingWriter {

  private OutputStream out;

  private int buffer = 0;
  private int count = 0;

  public SixBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 6;
    buffer |= val;
    ++ count;
    if (count == 4) {
      out.write((buffer >>> 16) & 0xFF);
      out.write((buffer >>>  8) & 0xFF);
      out.write((buffer >>>  0) & 0xFF);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    if (count != 0) {
      int numberOfBits = count * 6;
      finish(numberOfBits, buffer, out);
      buffer = 0;
      count = 0;
    }
    // check this does not impede perf
    out = null;
  }

}
class SixBitPackingReader extends BaseBitPackingReader {

  private final InputStream in;
  private final long valueCount;

  private int buffer = 0;
  private int count = 0;

  private long totalRead = 0;


  public SixBitPackingReader(InputStream in, long valueCount) {
    this.in = in;
    this.valueCount = valueCount;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      if (valueCount - totalRead < 4) {
        buffer = 0;
        int bitsToRead = 6 * (int)(valueCount - totalRead);
        int bytesToRead = alignToBytes(bitsToRead);
        for (int i = 3 - 1; i >= 3 - bytesToRead ; i--) {
          buffer |= in.read() << (i * 8);
        }
        count = 4;
        totalRead = valueCount;
      } else {
        buffer = (in.read() << 16) + (in.read() << 8) + in.read();
        count = 4;
        totalRead += 4;
      }
    }
    int result = (buffer >> ((count - 1) * 6)) & 63;
    -- count;
    return result;
  }

}

class SevenBitPackingWriter extends BaseBitPackingWriter {

  private OutputStream out;

  private long buffer = 0;
  private int count = 0;

  public SevenBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    buffer = buffer << 7;
    buffer |= val;
    ++ count;
    if (count == 8) {
      out.write((int)(buffer >>> 48) & 0xFF);
      out.write((int)(buffer >>> 40) & 0xFF);
      out.write((int)(buffer >>> 32) & 0xFF);
      out.write((int)(buffer >>> 24) & 0xFF);
      out.write((int)(buffer >>> 16) & 0xFF);
      out.write((int)(buffer >>>  8) & 0xFF);
      out.write((int)(buffer >>>  0) & 0xFF);
      buffer = 0;
      count = 0;
    }
  }

  @Override
  public void finish() throws IOException {
    if (count != 0) {
      int numberOfBits = count * 7;
      finish(numberOfBits, buffer, out);
      buffer = 0;
      count = 0;
    }
    // check this does not impede perf
    out = null;
  }

}
class SevenBitPackingReader extends BaseBitPackingReader {

  private final InputStream in;
  private final long valueCount;

  private long buffer = 0;
  private int count = 0;
  private long totalRead = 0;


  public SevenBitPackingReader(InputStream in, long valueCount) {
    this.in = in;
    this.valueCount = valueCount;
  }

  @Override
  public int read() throws IOException {
    if (count == 0) {
      if (valueCount - totalRead  < 8) {
        buffer = 0;
        int bitsToRead = 7 * (int)(valueCount - totalRead);
        int bytesToRead = alignToBytes(bitsToRead);
        for (int i = 7 - 1; i >= 7 - bytesToRead ; i--) {
          buffer |= (((long)in.read()) & 255) << (i * 8);
        }
        count = 8;
        totalRead = valueCount;
      } else {
        buffer =
            ((((long)in.read()) & 255) << 48)
            + ((((long)in.read()) & 255) << 40)
            + ((((long)in.read()) & 255) << 32)
            + ((((long)in.read()) & 255) << 24)
            + (in.read() << 16)
            + (in.read() << 8)
            + in.read();
        count = 8;
        totalRead += 8;
      }
    }
    int result = (((int)(buffer >> ((count - 1) * 7))) & 127);
    -- count;
    return result;
  }

}

class EightBitPackingWriter extends BitPackingWriter {

  private OutputStream out;

  public EightBitPackingWriter(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int val) throws IOException {
    out.write(val);
  }

  @Override
  public void finish() throws IOException {
    // check this does not impede perf
    out = null;
  }

}
class EightBitPackingReader extends BitPackingReader {

  private final InputStream in;

  public EightBitPackingReader(InputStream in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

}