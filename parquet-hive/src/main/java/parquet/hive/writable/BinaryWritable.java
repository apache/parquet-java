/**
 * Copyright 2013 Criteo.
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
package parquet.hive.writable;

import org.apache.hadoop.io.BytesWritable;

import parquet.io.api.Binary;

/**
 *
 * A Wrapper to support constructor with Binary and String
 *
 * TODO : remove it, and call BytesWritable with the getBytes()
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class BinaryWritable extends BytesWritable {

  public BinaryWritable(final Binary binary) {
    super(binary.getBytes());
  }

  public BinaryWritable(final String string) {
    super(string.getBytes());
  }

  public BinaryWritable() {
    super();
  }
}
