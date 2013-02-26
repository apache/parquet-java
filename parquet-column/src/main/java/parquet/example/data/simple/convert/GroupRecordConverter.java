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
package parquet.example.data.simple.convert;

import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.schema.MessageType;

public class GroupRecordConverter extends SimpleGroupConverter {

  private final SimpleGroupFactory simpleGroupFactory;

  private Group current;

  public GroupRecordConverter(MessageType schema) {
    super(null, -1, schema);
    this.simpleGroupFactory = new SimpleGroupFactory(schema);
  }

  @Override
  public Group getCurrentRecord() {
    return current;
  }

  @Override
  public void start() {
    current = simpleGroupFactory.newGroup();
  }

  @Override
  public void end() {
  }

}
