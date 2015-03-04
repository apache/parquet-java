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
/**
 *
 * <p>
 * Provides classes to store Avro data in Parquet files. Avro schemas are converted to
 * parquet schemas as follows. Only record schemas are converted,
 * other top-level schema types are not converted and attempting to do so will result
 * in an error. Avro types are converted to Parquet types using the mapping shown here:
 * </p>
 *
 * <table>
 *   <tr>
 *     <th>Avro type</th>
 *     <th>Parquet type</th>
 *   </tr>
 *   <tr>
 *     <td>null</td>
 *     <td>no type (the field is not encoded in Parquet), unless a null union</td>
 *   </tr>
 *   <tr>
 *     <td>boolean</td>
 *     <td>boolean</td>
 *   </tr>
 *   <tr>
 *     <td>int</td>
 *     <td>int32</td>
 *   </tr>
 *   <tr>
 *     <td>long</td>
 *     <td>int64</td>
 *   </tr>
 *   <tr>
 *     <td>float</td>
 *     <td>float</td>
 *   </tr>
 *   <tr>
 *     <td>double</td>
 *     <td>double</td>
 *   </tr>
 *   <tr>
 *     <td>bytes</td>
 *     <td>binary</td>
 *   </tr>
 *   <tr>
 *     <td>string</td>
 *     <td>binary (with original type UTF8)</td>
 *   </tr>
 *   <tr>
 *     <td>record</td>
 *     <td>group containing nested fields</td>
 *   </tr>
 *   <tr>
 *     <td>enum</td>
 *     <td>binary (with original type ENUM)</td>
 *   </tr>
 *   <tr>
 *     <td>array</td>
 *     <td>group (with original type LIST) containing one repeated group field</td>
 *   </tr>
 *   <tr>
 *     <td>map</td>
 *     <td>group (with original type MAP) containing one repeated group
 *     field (with original type MAP_KEY_VALUE) of (key, value)</td>
 *   </tr>
 *   <tr>
 *     <td>fixed</td>
 *     <td>fixed_len_byte_array</td>
 *   </tr>
 *   <tr>
 *     <td>union</td>
 *     <td>an optional type, in the case of a null union, otherwise not supported</td>
 *   </tr>
 * </table>
 *
 * <p>
 * For Parquet files that were not written with classes from this package there is no
 * Avro write schema stored in the Parquet file metadata. To read such files using
 * classes from this package you must either provide an Avro read schema,
 * or a default Avro schema will be derived using the following mapping.
 * </p>
 *
 *   <tr>
 *     <th>Parquet type</th>
 *     <th>Avro type</th>
 *   </tr>
 *   <tr>
 *     <td>boolean</td>
 *     <td>boolean</td>
 *   </tr>
 *   <tr>
 *     <td>int32</td>
 *     <td>int</td>
 *   </tr>
 *   <tr>
 *     <td>int64</td>
 *     <td>long</td>
 *   </tr>
 *   <tr>
 *     <td>int96</td>
 *     <td>not supported</td>
 *   </tr>
 *   <tr>
 *     <td>float</td>
 *     <td>float</td>
 *   </tr>
 *   <tr>
 *     <td>double</td>
 *     <td>double</td>
 *   </tr>
 *   <tr>
 *     <td>fixed_len_byte_array</td>
 *     <td>fixed</td>
 *   </tr>
 *   <tr>
 *     <td>binary (with no original type)</td>
 *     <td>bytes</td>
 *   </tr>
 *   <tr>
 *     <td>binary (with original type UTF8)</td>
 *     <td>string</td>
 *   </tr>
 *   <tr>
 *     <td>binary (with original type ENUM)</td>
 *     <td>string</td>
 *   </tr>
 *   <tr>
 *     <td>group (with original type LIST) containing one repeated group field</td>
 *     <td>array</td>
 *   </tr>
 *   <tr>
 *     <td>group (with original type MAP) containing one repeated group
 *     field (with original type MAP_KEY_VALUE) of (key, value)</td>
 *     <td>map</td>
 *   </tr>
 * </table>
 *
 * <p>
 * Parquet fields that are optional are mapped to an Avro null union.
 * </p>
 *
 * <p>
 * Some conversions are lossy. Avro nulls are not represented in Parquet,
 * so they are lost when converted back to Avro. Similarly, a Parquet enum does not
 * store its values, so it cannot be converted back to an Avro enum,
 * which is why an Avro string had to suffice. Type names for nested records, enums,
 * and fixed types are lost in the conversion to Parquet.
 * Avro aliases, default values, field ordering, and documentation strings are all
 * dropped in the conversion to Parquet.
 *
 * Parquet maps can have any type for keys, but this is not true in Avro where map keys
 * are assumed to be strings.
 * </p>
 */
package parquet.avro;
