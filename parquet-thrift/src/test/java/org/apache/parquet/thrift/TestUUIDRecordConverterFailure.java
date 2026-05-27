/**
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
package org.apache.parquet.thrift;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

// Test to verify UUID converter support in ThriftRecordConverter.
public class TestUUIDRecordConverterFailure {

  @Test
  public void testUUIDConverterSupport() {
    ThriftType.UUIDType uuidType = new ThriftType.UUIDType();
    ThriftField uuidField = new ThriftField("id", (short) 1, ThriftField.Requirement.REQUIRED, uuidType);

    UUID testUUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    byte[] uuidBytes = uuidToBytes(testUUID);
    Binary binary = Binary.fromConstantByteArray(uuidBytes);

    ThriftRecordConverter.FieldUUIDConverter uuidConverter =
        new ThriftRecordConverter.FieldUUIDConverter(new ArrayList<TProtocol>(), uuidField);

    try {
      uuidConverter.addBinary(binary);
      assertTrue("UUID converter handled binary data", true);
    } catch (UnsupportedOperationException e) {
      fail("UUID converter should handle binary data, but got: " + e.getMessage());
    }
  }

  private byte[] uuidToBytes(UUID uuid) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }
}
