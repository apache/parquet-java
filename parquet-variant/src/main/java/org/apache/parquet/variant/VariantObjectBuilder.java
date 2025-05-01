/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.variant;

import java.util.ArrayList;

/**
 * Builder for creating Variant object, used by VariantBuilder.
 */
public class VariantObjectBuilder extends VariantBuilder {
  /** The parent VariantBuilder. */
  private final VariantBuilder parent;
  /** The FieldEntry list for the fields of this object. */
  private final ArrayList<VariantBuilder.FieldEntry> fields;

  VariantObjectBuilder(VariantBuilder parent) {
    this.parent = parent;
    this.fields = new ArrayList<>();
  }

  /**
   * Appends an object key to this object. This method must be called before appending any value.
   * @param key the key to append
   */
  void appendKey(String key) {
    if (fields.size() > numValues) {
      throw new IllegalStateException("Cannot call appendKey() before appending an object value.");
    }
    updateLastValueSize();
    fields.add(new VariantBuilder.FieldEntry(key, addDictionaryKey(key), writePos));
  }

  /**
   * Returns the list of FieldEntry in this object. The state of the object is validated, so this
   * object is guaranteed to have the same number of keys and values.
   * @return the list of fields in this object
   */
  ArrayList<VariantBuilder.FieldEntry> validateAndGetFields() {
    if (fields.size() != numValues) {
      throw new IllegalStateException(String.format(
          "Number of object keys (%d) do not match the number of values (%d).", fields.size(), numValues));
    }
    updateLastValueSize();
    return fields;
  }

  @Override
  protected void checkAppendState() {
    if (objectBuilder != null) {
      throw new IllegalStateException(
          "Cannot call append() methods while an object is being built. Must call endObject() first.");
    }
    if (numValues != fields.size() - 1) {
      throw new IllegalStateException("Cannot append an object value before calling appendKey()");
    }
  }

  @Override
  int addDictionaryKey(String key) {
    // Add to the parent dictionary.
    return parent.addDictionaryKey(key);
  }

  private void updateLastValueSize() {
    if (!fields.isEmpty()) {
      VariantBuilder.FieldEntry lastField = fields.get(fields.size() - 1);
      lastField.updateValueSize(writePos() - lastField.offset);
    }
  }
}
