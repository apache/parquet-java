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
package parquet.io;

import static org.junit.Assert.assertEquals;

import java.util.Deque;

final public class ExpectationValidatingRecordConsumer extends
    RecordMaterializer<Void> {
  private final Deque<String> expectations;
  int count = 0;

  public ExpectationValidatingRecordConsumer(Deque<String> expectations) {
    this.expectations = expectations;
  }

  private void validate(String got) {
//    System.out.println("  \"" + got + "\";");
    assertEquals("event #"+count, expectations.pop(), got);
    ++count;
  }

  @Override
  public void startMessage() {
    validate("startMessage()");
  }

  @Override
  public void startGroup() {
    validate("startGroup()");
  }

  @Override
  public void startField(String field, int index) {
    validate("startField("+field+", "+index+")");
  }

  @Override
  public void endMessage() {
    validate("endMessage()");
  }

  @Override
  public void endGroup() {
    validate("endGroup()");
  }

  @Override
  public void endField(String field, int index) {
    validate("endField("+field+", "+index+")");
  }

  @Override
  public void addInteger(int value) {
    validate("addInt("+value+")");
  }

  @Override
  public void addLong(long value) {
    validate("addLong("+value+")");
  }

  @Override
  public void addBoolean(boolean value) {
    validate("addBoolean("+value+")");
  }

  @Override
  public void addBinary(Binary value) {
    validate("addBinary("+value.toStringUsingUTF8()+")");
  }

  @Override
  public void addFloat(float value) {
    validate("addFloat("+value+")");
  }

  @Override
  public void addDouble(double value) {
    validate("addDouble("+value+")");
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }
}