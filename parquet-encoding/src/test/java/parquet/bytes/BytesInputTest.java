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
package parquet.bytes;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;


public class BytesInputTest {
  @Test
  public void shouldWriteZigZagEncodedData() throws IOException {
    for (int value : randomIntegers(1000)) {
      shouldWriteZigZagValue(value);
    }
    shouldWriteZigZagValue(Integer.MAX_VALUE);
    shouldWriteZigZagValue(Integer.MIN_VALUE);
    shouldWriteZigZagValue(0);
  }

  private void shouldWriteZigZagValue(int value) throws IOException {
    BytesInput b = BytesInput.fromZigZagVarInt(value);
    byte[] bytes = b.toByteArray();
    ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
    assertEquals(bytes.length, stream.available());
    int valueRead = BytesUtils.readZigZagVarInt(stream);
    assertEquals(0,stream.available());
    assertEquals(value, valueRead);
  }

  private int[] randomIntegers(int size){
    int[] data = new int[size];
    Random random =new Random();
    for(int i=0;i<size;i++){
       data[i]=random.nextInt();
    }
    return data;
  }
}
