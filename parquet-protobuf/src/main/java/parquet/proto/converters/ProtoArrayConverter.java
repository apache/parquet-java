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

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

public class ProtoArrayConverter extends GroupConverter {

  private final Converter converter;

  public ProtoArrayConverter(Converter innerConverter) {
    converter = innerConverter;
  }

  @Override
  public Converter getConverter(int i) {
    return converter;
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {

  }
}
