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
package redelm.bytes;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import redelm.Log;

public class BytesUtils {
  private static final Log LOG = Log.getLog(BytesUtils.class);

  public static final Charset UTF8 = Charset.forName("UTF-8");

  public static int getWidthFromMaxInt(int bound) {
    return (int)Math.ceil(Math.log(bound + 1)/Math.log(2));
  }

  public static int readIntBigEndian(byte[] in, int offset) throws IOException {
    int ch1 = in[offset] & 0xff;
    int ch2 = in[offset + 1] & 0xff;
    int ch3 = in[offset + 2] & 0xff;
    int ch4 = in[offset + 3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static int readIntLittleEndian(InputStream in) throws IOException {
    int ch1 = in.read();
    int ch2 = in.read();
    int ch3 = in.read();
    int ch4 = in.read();
    if ((ch1 | ch2 | ch3 | ch4) < 0)
        throw new EOFException();
    if (Log.DEBUG) LOG.debug("read le int: " + ch1 + " " + ch2 + " " + ch3 + " " + ch4 + " => " + ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0)));
    return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
}

  public static void writeIntBigEndian(OutputStream out, int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }

  public static void writeIntLittleEndian(OutputStream out, int v) throws IOException {
    out.write((v >>>  0) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 24) & 0xFF);
    if (Log.DEBUG) LOG.debug("write le int: " + v + " => "+ ((v >>>  0) & 0xFF) + " " + ((v >>>  8) & 0xFF) + " " + ((v >>> 16) & 0xFF) + " " + ((v >>> 24) & 0xFF));
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

  public static void writeUnsignedVarInt(int value, OutputStream out) throws IOException {
    while ((value & 0xFFFFFF80) != 0L) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value & 0x7F);
  }
}
