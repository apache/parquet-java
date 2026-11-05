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

package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.BytesInput;
import org.junit.jupiter.api.Test;

public class TestFullBodyEncryptedDataPageV2 {

  /** The serialized form of an encrypted module: its length, then that many bytes. */
  private static byte[] module(int contentLength) {
    ByteBuffer module = ByteBuffer.allocate(Integer.BYTES + contentLength).order(ByteOrder.LITTLE_ENDIAN);
    module.putInt(contentLength);
    for (int i = 0; i < contentLength; i++) {
      module.put((byte) (0x80 + i));
    }
    return module.array();
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] result = new byte[first.length + second.length];
    System.arraycopy(first, 0, result, 0, first.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  @Test
  public void bodyThatIsOneModuleIsRecognized() throws IOException {
    byte[] body = module(50);

    assertThat(FullBodyEncryptedDataPageV2.isSingleEncryptedModule(BytesInput.from(body)))
        .isTrue();
  }

  @Test
  public void bodyWithPlaintextLevelsInFrontOfAModuleIsRecognized() throws IOException {
    // RLE encoded levels: a bit-packed run of one group, 0xff, followed by an RLE run of 8 values of 1
    byte[] levels = {0x03, (byte) 0xff, 0x10, 0x01};
    byte[] body = concat(levels, module(50));

    assertThat(FullBodyEncryptedDataPageV2.isSingleEncryptedModule(BytesInput.from(body)))
        .isFalse();
  }

  @Test
  public void bodyThatIsOneModuleIsRecognizedWhenItIsNotInOneBuffer() throws IOException {
    byte[] body = module(300);
    ByteBuffer first = ByteBuffer.wrap(body, 0, 2).slice();
    ByteBuffer second = ByteBuffer.wrap(body, 2, body.length - 2).slice();

    assertThat(FullBodyEncryptedDataPageV2.isSingleEncryptedModule(BytesInput.from(first, second)))
        .isTrue();
  }

  @Test
  public void bodyThatIsTooShortToStartWithALengthIsNotOneModule() throws IOException {
    assertThat(FullBodyEncryptedDataPageV2.isSingleEncryptedModule(BytesInput.empty()))
        .isFalse();
    assertThat(FullBodyEncryptedDataPageV2.isSingleEncryptedModule(BytesInput.from(new byte[] {1, 2, 3})))
        .isFalse();
  }

  @Test
  public void bodyIsNotConsumedByTheCheck() throws IOException {
    BytesInput body = BytesInput.from(module(40));

    FullBodyEncryptedDataPageV2.isSingleEncryptedModule(body);

    assertThat(body.toByteArray()).isEqualTo(module(40));
  }
}
