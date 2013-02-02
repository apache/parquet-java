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

import java.io.IOException;
import java.io.OutputStream;

public class BytesUtils {

  public static int readInt(byte[] in, int offset) throws IOException {
    int ch1 = in[offset] & 0xff;
    int ch2 = in[offset + 1] & 0xff;
    int ch3 = in[offset + 2] & 0xff;
    int ch4 = in[offset + 3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static void writeInt(OutputStream out, int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }
}
