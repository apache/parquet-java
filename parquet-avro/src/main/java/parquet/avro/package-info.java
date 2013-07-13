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
 */
package parquet.avro;
