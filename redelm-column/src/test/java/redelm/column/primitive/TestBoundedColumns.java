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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Random;

import org.junit.Test;

public class TestBoundedColumns {
  private final Random r = new Random(42L);

  @Test
  public void testSerDe() throws Exception {
    int[] valuesPerStripe = new int[] { 50, 1000, 700, 1, 2000 };
    int totalValuesInStream = 0;
    for (int v : valuesPerStripe) {
      totalValuesInStream += v * 2;
    }

    for (int bound = 1; bound < 20; bound++) {
      File tmp = File.createTempFile("tmp", "tmp");
      DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmp));

      int[] stream = new int[totalValuesInStream];
      BoundedIntColumnWriter bicw = new BoundedIntColumnWriter(bound);
      int idx = 0;
      for (int stripeNum = 0; stripeNum < valuesPerStripe.length; stripeNum++) {
        int next = 0;
        for (int i = 0; i < valuesPerStripe[stripeNum]; i++) {
          int temp = r.nextInt(bound + 1);
          while (next == temp) {
            temp = r.nextInt(bound + 1);
          }
          next = temp;
          stream[idx++] = next;
          int ct;
          if (r.nextBoolean()) {
            stream[idx++] = ct = r.nextInt(1000) + 1;
          } else {
            stream[idx++] = ct = 1;
          }
          for (int j = 0; j < ct; j++) {
            bicw.writeInteger(next);
          }
        }
        bicw.writeData(dos);
        bicw.reset();
      }
      dos.close();

      DataInputStream dis = new DataInputStream(new FileInputStream(tmp));

      BoundedIntColumnReader bicr = new BoundedIntColumnReader(bound);
      idx = 0;
      for (int stripeNum = 0; stripeNum < valuesPerStripe.length; stripeNum++) {
        bicr.readStripe(dis);
        for (int i = 0; i < valuesPerStripe[stripeNum]; i++) {
          int number = stream[idx++];
          int ct = stream[idx++];
          assertTrue(number <= bound);
          assertTrue(ct > 0);
          for (int j = 0; j < ct; j++) {
            assertEquals("Failed on bound ["+bound+"], stripe ["+stripeNum+"], iteration ["+i+"], on count ["+ct+"]", number, bicr.readInteger());
          }
        }
      }
    }
  }
}
