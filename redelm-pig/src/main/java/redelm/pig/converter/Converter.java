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

public abstract class Converter {

  private Converter parent;

  Converter(Converter parent) {
    super();
    this.parent = parent;
  }

  abstract public void start();

  public Converter end() {
    return parent;
  }

  abstract public void startField(String field, int index);

  abstract public void endField(String field, int index);

  public Converter getParent() {
    return parent;
  }

  abstract public Converter startGroup();

  abstract public void endGroup();

  abstract public Object get();

  abstract public void set(Object value);

  abstract public void toString(String indent, StringBuffer sb);

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    toString("", sb);
    return sb.toString();
  }
}
