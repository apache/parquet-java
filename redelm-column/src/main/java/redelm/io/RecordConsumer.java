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
package redelm.io;

abstract public class RecordConsumer {

  abstract public void startMessage();
  abstract public void endMessage();

  abstract public void startField(String field, int index);
  abstract public void endField(String field, int index);

  abstract public void startGroup();
  abstract public void endGroup();

  abstract public void addInteger(int value);
  abstract public void addLong(long value);
  abstract public void addString(String value);
  abstract public void addBoolean(boolean value);
  abstract public void addBinary(byte[] value);
  abstract public void addFloat(float value);
  abstract public void addDouble(double value);

}
