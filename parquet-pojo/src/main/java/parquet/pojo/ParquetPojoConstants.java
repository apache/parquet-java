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
package parquet.pojo;

/**
 * Constants for pojo support
 */
public class ParquetPojoConstants {
  /**
   * Name of the top level class used in resolving classes from input
   */
  public static String PARQUET_POJO_INPUT_CLASS_KEY = "parquet.pojo.input.class";

  /**
   * Name of the top level class used in resolving classes from output
   */
  public static String PARQUET_POJO_OUTPUT_CLASS_KEY = "parquet.pojo.output.class";

  /**
   * Name of the key class of the top level map
   */
  public static String PARQUET_POJO_INPUT_MAP_KEY_CLASS = "parquet.pojo.input.map.key.class";
  /**
   * Name of the value class of the top level map
   */
  public static String PARQUET_POJO_INPUT_MAP_VALUE_CLASS = "parquet.pojo.input.map.value.class";
  /**
   * Name of the value class of the top level map
   */
  public static String PARQUET_POJO_INPUT_LIST_VALUE_CLASS = "parquet.pojo.input.list.value.class";

  /**
   * Name of the key class of the top level map
   */
  public static String PARQUET_POJO_OUTPUT_MAP_KEY_CLASS = "parquet.pojo.output.map.key.class";
  /**
   * Name of the value class of the top level map
   */
  public static String PARQUET_POJO_OUTPUT_MAP_VALUE_CLASS = "parquet.pojo.output.map.value.class";
  /**
   * Name of the value class of the top level map
   */
  public static String PARQUET_POJO_OUTPUT_LIST_VALUE_CLASS = "parquet.pojo.output.list.value.class";
}