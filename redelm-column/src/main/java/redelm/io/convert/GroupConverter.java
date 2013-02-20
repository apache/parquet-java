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
package redelm.io.convert;

/**
 * Represent a tree of converters
 * that materializes tuples
 *
 * @author Julien Le Dem
 *
 */
public abstract class GroupConverter {

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of a group field in this group
   * @return the corresponding converter
   */
  abstract public GroupConverter getGroupConverter(int fieldIndex);

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of a primitive field in this group
   * @return the corresponding converter
   */
  abstract public PrimitiveConverter getPrimitiveConverter(int fieldIndex);


  /** runtime calls  **/

  /** called at the beginning of the group managed by this converter */
  public abstract void start();

  /**
   * call at the end of the group
   */
  public abstract void end();

}
