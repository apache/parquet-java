/**
 * Copyright 2013 Lukas Nalezenec.
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

package parquet.proto.converters;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

public final class ProtoStringConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoStringConverter(ParentValueContainer parent) {
    this.parent = parent;
  }


  @Override
  public void addBinary(Binary binary) {
    String str = binary.toStringUsingUTF8();
    parent.add(str);
  }

}
