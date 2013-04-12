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
package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class TestRLE {

  @Test
  public void testOne() throws IOException {
    int[] in = {0,0,1,0,1,1,1,0,1,0,1,1,1};
    verify(in, 1);
  }

  @Test
  public void testTwo() throws IOException {
    int[] in = {0,0,1,0,2,2,1,0,3,0,1,1,1};
    verify(in, 2);
  }

  @Test
  public void testThree() throws IOException {
    int[] in = {0,4,1,0,2,2,1,0,3,0,1,5,1,6,7};
    verify(in, 3);
  }

  private void verify(int[] in, int width) throws IOException {
    final RLESimpleEncoder rleSimpleEncoder = new RLESimpleEncoder(width);
    for (int i : in) {
      rleSimpleEncoder.writeInt(i);
    }
    final RLEDecoder rleDecoder = new RLEDecoder(width, new ByteArrayInputStream(rleSimpleEncoder.toBytes().toByteArray()));
    for (int i = 0; i < in.length; i++) {
      Assert.assertEquals(in[i], rleDecoder.readInt());
    }
  }
}
