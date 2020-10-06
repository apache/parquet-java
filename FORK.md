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

# Differences to mainline
This fork has been created to convert protobuf datatypes while writing into parquet. This is done to make sure that we can load data from parquet files into Google Bigquery without any transformations.

Here is a list of changes made in the repository:

a. [PARQUET-1885](https://issues.apache.org/jira/browse/PARQUET-1885): Use Protobuf descriptor from constructor rather than from class path. More details are present in the issue.

b. Data type conversions for following data type:

1. google.protobuf.Timestamp: This descriptor is a nested field with two fields seconds and nanos, we write one value of type long denoting epoch timestamp in milliseconds, it is also written with LogicalAnnotation of TIMESTAMP_MILLIS. This facilitates using timestamp fields as TIMESTAMP type in bigquery rather than nested type. More info: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#type_conversions

2. google.protobuf.Struct: There is an open issue in parquet-protobuf regarding writing recursive datatypes in parquet. Since struct is a frequently used datatype in gojek, we have decided to write the Struct datatype as String so as to get around with recursive data type.
Link to the issue: [PARQUET-1711](https://issues.apache.org/jira/browse/PARQUET-1711)

3. Enum: While loading Enums from parquet to bigquery, we observed that they are loaded as bytes data type and not as string, thus we are writing enums as string so it can be imported into bigquery as string
