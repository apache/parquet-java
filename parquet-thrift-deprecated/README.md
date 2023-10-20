<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Apache Thrift Integration

**Todo:** Add a description and examples on how to use Parquet-Thrift integration.

## Available options via Hadoop Configuration

### Configuration for reading
**Todo:** Add read configs

### Configuration for writing
**Todo:** Add all write configs
| Name                                    | Type      | Description                                                          |
|-----------------------------------------|-----------|----------------------------------------------------------------------|
| `parquet.thrift.write-three-level-lists`| `boolean` | Write lists with 3-level structure to allow list and list elements to be nullable. When set to `true`, lists will be written as per https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists|
