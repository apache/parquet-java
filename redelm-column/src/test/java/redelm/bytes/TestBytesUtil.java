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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class TestBytesUtil {

  @Test
  public void testInt() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BytesUtils.writeIntBigEndian(baos, 1208);
    int readInt = BytesUtils.readIntBigEndian(baos.toByteArray(), 0);
    assertEquals(1208, readInt);
  }

  @Test
  public void testReadInt() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataOutputStream(baos).writeInt(1208);
    int readInt = BytesUtils.readIntBigEndian(baos.toByteArray(), 0);
    assertEquals(1208, readInt);
  }

  @Test
  public void testWriteInt() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BytesUtils.writeIntBigEndian(baos, 1208);
    int readInt = new DataInputStream(new ByteArrayInputStream(baos.toByteArray())).readInt();
    assertEquals(1208, readInt);
  }

}
