/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.io.api;

import parquet.column.Dictionary;

/**
 * converter for leaves of the schema
 *
 * @author Julien Le Dem
 *
 */
abstract public class PrimitiveConverter extends Converter {

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public PrimitiveConverter asPrimitiveConverter() {
    return this;
  }

  /**
   * if it returns true we will attempt to use dictionary based conversion instead
   * @return if dictionary is supported
   */
  public boolean hasDictionarySupport() {
    return false;
  }

  /**
   * Set the dictionary to use if the data was encoded using dictionary encoding
   * and the converter hasDictionarySupport().
   * @param dictionary the dictionary to use for conversion
   */
  public void setDictionary(Dictionary dictionary) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** runtime calls  **/

  /**
   * add a value based on the dictionary set with setDictionary()
   * Will be used if the Converter has dictionary support and the data was encoded using a dictionary
   * @param dictionaryId the id in the dictionary of the value to add
   */
  public void addValueFromDictionary(int dictionaryId) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addBinary(Binary value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addBoolean(boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addDouble(double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addFloat(float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addInt(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addLong(long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

}
