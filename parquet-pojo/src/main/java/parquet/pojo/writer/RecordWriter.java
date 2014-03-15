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
package parquet.pojo.writer;

import parquet.io.api.RecordConsumer;
import parquet.pojo.field.FieldAccessor;

/**
 * Writes objects to the underlying consumer
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public interface RecordWriter {
  /**
   * Writes values and any dependent fields / groups, but does not write the containing field.
   *
   * @param value
   * @param recordConsumer
   */
  void writeValue(Object value, RecordConsumer recordConsumer);

  /**
   * Writes values / dependent fields as well as the containing field. Uses the <code>fieldAccessor</code> to extract the correct field from
   * the <code>parent</code>
   *
   * @param parent
   * @param recordConsumer
   * @param index
   * @param fieldAccessor
   */
  void writeFromField(
    Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
  );
}
