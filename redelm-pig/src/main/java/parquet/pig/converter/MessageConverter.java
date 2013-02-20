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
package parquet.pig.converter;


import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import parquet.io.RecordMaterializer;
import parquet.schema.MessageType;

public class MessageConverter extends TupleConverter {

  private static final class TupleRecordConsumer extends RecordMaterializer<Tuple> {
    private Converter currentConverter;
    private Tuple currentTuple;

    public TupleRecordConsumer(MessageConverter messageConverter) {
      this.currentConverter = messageConverter;
    }

    @Override
    public void startMessage() {
      currentConverter.start();
    }

    @Override
    public void startGroup() {
      this.currentConverter = currentConverter.startGroup();
      currentConverter.start();
    }

    @Override
    public void startField(String field, int index) {
      currentConverter.startField(field, index);
    }

    @Override
    public void endMessage() {
      this.currentTuple = (Tuple)currentConverter.get();
    }

    @Override
    public void endGroup() {
      currentConverter = currentConverter.end();
      this.currentConverter.endGroup();
    }

    @Override
    public void endField(String field, int index) {
      currentConverter.endField(field, index);
    }

    @Override
    public void addLong(long value) {
      currentConverter.set(value);
    }

    @Override
    public void addInteger(int value) {
      currentConverter.set(value);
    }

    @Override
    public void addFloat(float value) {
      currentConverter.set(value);
    }

    @Override
    public void addDouble(double value) {
      currentConverter.set(value);
    }

    @Override
    public void addBoolean(boolean value) {
      currentConverter.set(value);
    }

    @Override
    public void addBinary(byte[] value) {
      currentConverter.set(value);
    }

    @Override
    public Tuple getCurrentRecord() {
      return currentTuple;
    }
  }

  public MessageConverter(MessageType redelmSchema, Schema pigSchema) throws FrontendException {
    super(redelmSchema, pigSchema, null);
  }

  public RecordMaterializer<Tuple> newRecordConsumer() {
    return new TupleRecordConsumer(this);
  }

  @Override
  public Converter end() {
    throw new UnsupportedOperationException("bad input. Can not close the message converter");
  }

}
