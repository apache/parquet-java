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
package redelm.pig.converter;

import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import redelm.io.RecordConsumer;
import redelm.schema.MessageType;

public class MessageConverter extends TupleConverter {

  private static final class TupleRecordConsumer extends RecordConsumer {
    private Converter currentConverter;
    private final List<Tuple> destination;

    public TupleRecordConsumer(MessageConverter messageConverter, List<Tuple> destination) {
      this.currentConverter = messageConverter;
      this.destination = destination;
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
      currentConverter.end();
      destination.add((Tuple)currentConverter.get());
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
    public void addString(String value) {
      currentConverter.set(value);
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
  }

  public MessageConverter(MessageType redelmSchema, Schema pigSchema) throws FrontendException {
    super(redelmSchema, pigSchema, null);
  }

  public RecordConsumer newRecordConsumer(List<Tuple> destination) {
    return new TupleRecordConsumer(this, destination);
  }
}
