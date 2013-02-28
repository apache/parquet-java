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
package parquet.hadoop.thrift;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;

public class ParquetReadProtocol extends ParquetProtocol {

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readMessageBegin()
   */
  @Override
  public TMessage readMessageBegin() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readMessageEnd()
   */
  @Override
  public void readMessageEnd() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readStructBegin()
   */
  @Override
  public TStruct readStructBegin() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readStructEnd()
   */
  @Override
  public void readStructEnd() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readFieldBegin()
   */
  @Override
  public TField readFieldBegin() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readFieldEnd()
   */
  @Override
  public void readFieldEnd() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readMapBegin()
   */
  @Override
  public TMap readMapBegin() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readMapEnd()
   */
  @Override
  public void readMapEnd() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readListBegin()
   */
  @Override
  public TList readListBegin() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readListEnd()
   */
  @Override
  public void readListEnd() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readSetBegin()
   */
  @Override
  public TSet readSetBegin() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readSetEnd()
   */
  @Override
  public void readSetEnd() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readBool()
   */
  @Override
  public boolean readBool() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readByte()
   */
  @Override
  public byte readByte() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readI16()
   */
  @Override
  public short readI16() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readI32()
   */
  @Override
  public int readI32() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readI64()
   */
  @Override
  public long readI64() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readDouble()
   */
  @Override
  public double readDouble() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readString()
   */
  @Override
  public String readString() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * {@inheritDoc}
   * @see parquet.hadoop.thrift.ParquetProtocol#readBinary()
   */
  @Override
  public ByteBuffer readBinary() throws TException {
    throw new UnsupportedOperationException("NYI");
  }

}
