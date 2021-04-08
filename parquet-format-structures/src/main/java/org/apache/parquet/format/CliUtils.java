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

package org.apache.parquet.format;

import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

/**
 * Utility class for parquet-cli. This is required because this module shades thriftlib which means we cannot use Thrift
 * classes outside of this module without adding thriftlib as a separate dependency.
 */
public class CliUtils {

  /**
   * Returns the json representation of the specified Thrift object
   *
   * @param tbase the thrift object to be serialized as a json
   * @return the json representation of the Thrift object as a String
   * @throws IOException if any Thrift error occurs during the serialization
   */
  public static String toJson(TBase<?, ?> tbase) throws IOException {
    try {
      TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      return serializer.toString(tbase);
    } catch (TException e) {
      // Wrapping the exception the not to expose the shaded Thrift class TException
      throw new IOException(e);
    }
  }

  private CliUtils() {
    // private constructor to avoid instantiation
  }
}
