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
package org.apache.parquet.format.event;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

/**
 * To receive Thrift field events
 */
public interface FieldConsumer {

  /**
   * called by the EventBasedThriftReader when reading a field from a Struct
   * @param protocol the underlying protocol
   * @param eventBasedThriftReader the reader to delegate to further calls.
   * @param id the id of the field
   * @param type the type of the field
   * @throws TException if any thrift related error occurs during the reading
   */
  public void consumeField(TProtocol protocol, EventBasedThriftReader eventBasedThriftReader, short id, byte type) throws TException;

}