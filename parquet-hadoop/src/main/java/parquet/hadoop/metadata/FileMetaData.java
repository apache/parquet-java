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
package parquet.hadoop.metadata;

import java.io.Serializable;

import parquet.schema.MessageType;


/**
 * File level meta data (Schema, codec, ...)
 *
 * @author Julien Le Dem
 *
 */
public final class FileMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageType schema;

  public FileMetaData(MessageType schema) {
    super();
    this.schema = schema;
  }

  public MessageType getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "FileMetaData{schema: "+schema+"}";
  }
}