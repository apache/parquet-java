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

package org.apache.parquet.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code Self-Reference} module type and the {@link AesCipher#createSelfReferenceAAD}
 * AAD construction defined for FILE self-references (parquet-format PR #603).
 */
public class TestSelfReferenceAAD {

  private static final byte[] FILE_AAD = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

  @Test
  public void testSelfReferenceModuleTypeValue() {
    // The spec assigns module type 10 to Self-Reference.
    assertThat(ModuleType.SelfReference.getValue()).isEqualTo((byte) 10);
  }

  @Test
  public void testSelfReferenceAADLayout() {
    int rowGroupOrdinal = 3;
    int columnOrdinal = 7;
    long selfReferenceOffset = 0x0102030405060708L;

    byte[] aad = AesCipher.createSelfReferenceAAD(FILE_AAD, rowGroupOrdinal, columnOrdinal, selfReferenceOffset);

    // Layout: fileAAD | moduleType(1) | rowGroupOrdinal(2 LE) | columnOrdinal(2 LE) |
    //         selfReferenceOffset(8 LE)
    assertThat(aad.length).isEqualTo(FILE_AAD.length + 1 + 2 + 2 + 8);

    ByteBuffer buf = ByteBuffer.wrap(aad).order(ByteOrder.LITTLE_ENDIAN);
    byte[] filePart = new byte[FILE_AAD.length];
    buf.get(filePart);
    assertThat(filePart).isEqualTo(FILE_AAD);
    assertThat(buf.get()).isEqualTo((byte) 10); // module type
    assertThat(buf.getShort()).isEqualTo((short) rowGroupOrdinal);
    assertThat(buf.getShort()).isEqualTo((short) columnOrdinal);
    // The self-reference is identified by the 8-byte file offset of its stored representation,
    // unlike the 2-byte page ordinal.
    assertThat(buf.getLong()).isEqualTo(selfReferenceOffset);
  }

  @Test
  public void testSelfReferenceAADSupportsLargeOffset() {
    // File offsets routinely exceed the 2-byte page-ordinal range, so the field must be 8 bytes.
    long largeOffset = 5L * 1024 * 1024 * 1024;
    byte[] aad = AesCipher.createSelfReferenceAAD(FILE_AAD, 0, 0, largeOffset);
    ByteBuffer buf = ByteBuffer.wrap(aad, FILE_AAD.length + 1 + 2 + 2, 8).order(ByteOrder.LITTLE_ENDIAN);
    assertThat(buf.getLong()).isEqualTo(largeOffset);
  }

  @Test
  public void testDistinctOffsetsProduceDistinctAADs() {
    // Two self-references in the same column chunk are distinguished solely by their offsets.
    byte[] first = AesCipher.createSelfReferenceAAD(FILE_AAD, 1, 2, 1000L);
    byte[] second = AesCipher.createSelfReferenceAAD(FILE_AAD, 1, 2, 1064L);
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  public void testSelfReferenceAADRejectsNegativeValues() {
    assertThatThrownBy(() -> AesCipher.createSelfReferenceAAD(FILE_AAD, -1, 0, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> AesCipher.createSelfReferenceAAD(FILE_AAD, 0, -1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> AesCipher.createSelfReferenceAAD(FILE_AAD, 0, 0, -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSelfReferenceAADDiffersFromPageAAD() {
    // A self-reference and a data page in the same column must not share an AAD, because the module
    // type byte differs (and the trailing field differs in width and meaning).
    byte[] selfRefAAD = AesCipher.createSelfReferenceAAD(FILE_AAD, 1, 2, 0);
    byte[] dataPageAAD = AesCipher.createModuleAAD(FILE_AAD, ModuleType.DataPage, 1, 2, 0);
    assertThat(selfRefAAD).isNotEqualTo(dataPageAAD);
  }
}
