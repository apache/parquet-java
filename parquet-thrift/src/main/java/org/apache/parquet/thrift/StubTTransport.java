/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.thrift;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * A stub transport which implements the minimum amount needed for range/depth
 * validation within the thrift library to succeed.
 */
final class StubTTransport extends TTransport {

  static final StubTTransport INSTANCE = new StubTTransport();

  /**
   * There's no limits on recursion depth.
   */
  private static final TConfiguration CONFIGURATION = new TConfiguration(
      TConfiguration.DEFAULT_MAX_FRAME_SIZE, TConfiguration.DEFAULT_MAX_MESSAGE_SIZE, Integer.MAX_VALUE);

  private StubTTransport() {}

  @Override
  public void checkReadBytesAvailable(long l) {}

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void open() throws TTransportException {
    throw new TTransportException("unsupported");
  }

  @Override
  public void close() {}

  @Override
  public int read(byte[] bytes, int i, int i1) throws TTransportException {
    throw new TTransportException("unsupported");
  }

  @Override
  public void write(byte[] bytes, int i, int i1) throws TTransportException {
    throw new TTransportException("unsupported");
  }

  @Override
  public TConfiguration getConfiguration() {
    return CONFIGURATION;
  }

  @Override
  public void updateKnownMessageSize(long l) {}
}
