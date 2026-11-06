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
import org.apache.parquet.hadoop.FullBodyEncryptedDataPageV2.Layout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestFullBodyEncryptedDataPageV2 {

  /** A bit-packed run of 8 levels, then an RLE run of 8 levels. */
  private static final byte[] LEVELS = {0x03, (byte) 0xff, 0x10, 0x01};

  /** {@code contentLength} as a 4 byte LE int, followed by that many bytes. */
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
  public void bodyThatIsOneModuleIsASingleModule() throws IOException {
    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(module(50)), LEVELS.length))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void bodyWithPlaintextLevelsInFrontOfAModuleHasPlaintextLevels() throws IOException {
    byte[] body = concat(LEVELS, module(50));

    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(body), LEVELS.length))
        .isEqualTo(Layout.PLAINTEXT_LEVELS);
  }

  @Test
  public void bodyThatIsOneModuleIsRecognizedWhenItIsNotInOneBuffer() throws IOException {
    byte[] body = module(300);
    ByteBuffer first = ByteBuffer.wrap(body, 0, 2).slice();
    ByteBuffer second = ByteBuffer.wrap(body, 2, body.length - 2).slice();

    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(first, second), LEVELS.length))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void bodyWithPlaintextLevelsIsRecognizedWhenTheModuleLengthIsNotInOneBuffer() throws IOException {
    byte[] body = concat(LEVELS, module(300));
    // Split inside the module length
    ByteBuffer first = ByteBuffer.wrap(body, 0, LEVELS.length + 2).slice();
    ByteBuffer second = ByteBuffer.wrap(body, LEVELS.length + 2, body.length - LEVELS.length - 2)
        .slice();

    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(first, second), LEVELS.length))
        .isEqualTo(Layout.PLAINTEXT_LEVELS);
  }

  @Test
  public void bodyThatFitsBothLayoutsIsAmbiguous() throws IOException {
    // Levels whose first 4 bytes are bodySize - 4
    byte[] module = module(50);
    int bodySize = LEVELS.length + module.length;
    byte[] levels = ByteBuffer.allocate(LEVELS.length)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(bodySize - Integer.BYTES)
        .array();
    byte[] body = concat(levels, module);

    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(body), levels.length))
        .isEqualTo(Layout.AMBIGUOUS);
  }

  @Test
  public void bodyThatFitsNeitherLayoutIsTreatedAsASingleModuleSoThatDecryptionReportsIt() throws IOException {
    byte[] body = concat(LEVELS, module(50));
    body[LEVELS.length] ^= 0x01;

    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(body), LEVELS.length))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void bodyThatIsTooShortToHoldAModuleLengthIsNotAnythingElseThanASingleModule() throws IOException {
    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.empty(), 1))
        .isEqualTo(Layout.SINGLE_MODULE);
    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(new byte[] {1, 2, 3}), 1))
        .isEqualTo(Layout.SINGLE_MODULE);
    // Body is only levels
    assertThat(FullBodyEncryptedDataPageV2.detectLayout(BytesInput.from(LEVELS), LEVELS.length))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @ParameterizedTest
  @CsvSource({
    "'parquet-mr version 1.18.0 (build abc)', PLAINTEXT_LEVELS",
    "'parquet-mr version 1.8.1', PLAINTEXT_LEVELS",
    "'parquet-mr version 1.18.0-SNAPSHOT (build abc)', PLAINTEXT_LEVELS",
    "'parquet-mr version 1.19.0 (build abc)', SINGLE_MODULE",
    "'parquet-mr version 1.20.1', SINGLE_MODULE",
    "'parquet-mr version 2.0.0-SNAPSHOT', SINGLE_MODULE",
    "'parquet-cpp-arrow version 24.0.0', SINGLE_MODULE",
    "'parquet-rs version 57.0.0', SINGLE_MODULE",
    "'parquet-mr version 1.19.0-SNAPSHOT (build abc)', AMBIGUOUS",
    "'parquet-mr version 1.19.0rc1', AMBIGUOUS",
    "'parquet-mr version', AMBIGUOUS",
    "'parquet-mr version not-a-version', AMBIGUOUS",
    "'not a created_by', AMBIGUOUS",
    "'', AMBIGUOUS",
  })
  public void layoutWrittenByFollowsTheWriterVersion(String createdBy, Layout expected) {
    assertThat(FullBodyEncryptedDataPageV2.layoutWrittenBy(createdBy)).isEqualTo(expected);
  }

  @Test
  public void layoutWrittenByAnUnknownWriterIsAmbiguous() {
    assertThat(FullBodyEncryptedDataPageV2.layoutWrittenBy(null)).isEqualTo(Layout.AMBIGUOUS);
  }
}
