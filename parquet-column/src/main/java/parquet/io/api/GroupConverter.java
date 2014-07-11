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
package parquet.io.api;


/**
 * converter for group nodes
 *
 * @author Julien Le Dem
 *
 */
abstract public class GroupConverter extends Converter {

  @Override
  public final boolean isPrimitive() {
    return false;
  }

  @Override
  public final GroupConverter asGroupConverter() {
    return this;
  }

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of the field in this group
   * @return the corresponding converter
   */
  abstract public Converter getConverter(int fieldIndex);

  /** runtime calls  **/

  /** called at the beginning of the group managed by this converter */
  abstract public void start();

  /**
   * call at the end of the group
   */
  abstract public void end();

}
