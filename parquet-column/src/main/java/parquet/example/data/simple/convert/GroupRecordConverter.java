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
import parquet.io.convert.GroupConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.MessageType;

public class GroupRecordConverter extends RecordConverter<Group> {

  private final SimpleGroupFactory simpleGroupFactory;

  private SimpleGroupConverter root;

  public GroupRecordConverter(MessageType schema) {
    this.simpleGroupFactory = new SimpleGroupFactory(schema);
    this.root = new SimpleGroupConverter(null, 0, schema) {
      @Override
      public void start() {
        this.current = simpleGroupFactory.newGroup();
      }

      @Override
      public void end() {
      }
    };
  }

  @Override
  public Group getCurrentRecord() {
    return root.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}
