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
package parquet.hadoop.example;

import parquet.example.data.Group;
import parquet.hadoop.ParquetInputFormat;

/**
 * Example input format to read Parquet files
 *
 * This Input format uses a rather inefficient data model but works independently of higher level abstractions.
 *
 * @author Julien Le Dem
 *
 */
public class ExampleInputFormat extends ParquetInputFormat<Group> {

  public ExampleInputFormat() {
    super(GroupReadSupport.class);
  }

}
