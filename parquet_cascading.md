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

Parquet Cascading Integration
=============================

This document details the support of reading and writing parquet format from cascading.

1. Read and Write
==============

In [parquet-cascading](https://github.com/apache/parquet-mr/tree/master/parquet-cascading) sub-module, we provide support for reading/writing records of various data structures including Thrift(TBase), Scrooge and Tuples. Please refer to following sections for each data structures.

1.1 Thrift/TBase
------------
### Read Thrift Records from Parquet
[ParquetTbaseScheme](https://github.com/apache/parquet-mr/blob/master/parquet-cascading/src/main/java/org/apache/parquet/cascading/ParquetTBaseScheme.java) is the interface for reading thrift records in Parquet format. Providing a ParquetTbaseScheme as a parameter to the constructor of a source enables the program to read Thrift object(TBase), eg.

`
Scheme sourceScheme = new ParquetTBaseScheme(Name.class)
Tap source = new Hfs(sourceScheme, parquetInputPath);
`

In the above example Name is a thrift class that extends TBase. Under the hood parquet will generate a schema from the thrift class to decode the data. 

The thrift class is actually *optional* to initialize a ParquetTBaseScheme when the data is written as Thrift records in Parquet. When writing thrift records to parquet format, the Thrift class of the records is stored as meta-data in the footer of the parquet file. Therefore when reading the file, if a thrift class is not explicitly provided, Parquet will use the class name stored in the footer as the thrift class. 

### Write Thrift Records to Parquet
[ParquetTbaseScheme](https://github.com/apache/parquet-mr/blob/master/parquet-cascading/src/main/java/org/apache/parquet/cascading/ParquetTBaseScheme.java) can also be used by a sink. When used as a sink, the Thrift class of the records being written must be *explicitly* provided.

`
Scheme sinkScheme = new ParquetTBaseScheme(Name.class);
Tap sink = new Hfs(sinkScheme, parquetOutputPath);
`

For more concrete examples please refer to [TestParquetTBaseScheme](https://github.com/apache/parquet-mr/blob/master/parquet-cascading/src/test/java/org/apache/parquet/cascading/TestParquetTBaseScheme.java)

1.2 Scrooge
-----------
### Read Scrooge records from Parquet
Scrooge support is defined in a separate module called [parquet-scrooge](https://github.com/apache/parquet-mr/tree/master/parquet-scrooge). With [ParquetScroogeScheme](https://github.com/apache/parquet-mr/blob/master/parquet-scrooge/src/main/java/org/apache/parquet/scrooge/ParquetScroogeScheme.java), data can be read in the form of Scrooge objects which are more scala friendly.

`
Scheme sinkScheme = new ParquetScroogeScheme(Name.class);
Tap sink = new Hfs(sinkScheme, parquetOutputPath);
`

### Write Scrooge Records to Parquet(Not supported yet)

1.3 Tuples
----------
### Read Cascading Tuples from Parquet
Currently, the support for reading tuples is mainly(but not limited) for data written from pig scripts as pig tuples. More comprehensive support will be added, but in the mean time, there are some limitations to notice: Nested structures are not supported. If the data is written as thrift objects which have nested structure, it can not be read at current time. *Data to read must be in flat structure*. To read data as tuples, simply use [ParquetTupleScheme](https://github.com/apache/parquet-mr/blob/master/parquet-cascading/src/main/java/org/apache/parquet/cascading/ParquetTupleScheme.java):

`
Scheme sourceScheme = new ParquetTupleScheme(new Fields("last_name"));
Tap source = new Hfs(sourceScheme, parquetInputPath);
`

### Write Cascading Tuples to Parquet(coming soon)

For more examples please refer to [TestParquetTupleScheme](https://github.com/apache/parquet-mr/blob/master/parquet-cascading/src/test/java/org/apache/parquet/cascading/TestParquetTupleScheme.java)

2. Projection Pushdown
======================
One of the big benefit of using columnar format is to be able to read only a subset of columns when the full schema is huge. It saves times by not reading unused columns. 

Parquet support projection pushdown for Thrift records and tuples.

### 2.1 Projection Pushdown with Thrift/Scrooge Records
To read only a subset of columns in a Thrift/Scrooge class, the columns of interest should be specified using a glob syntax.

For example, imagine a Person struct defined as:

    struct Person {
      1: required string name
      2: optional int16 age
      3: optional Address primaryAddress
      4: required map<string, Address> otherAddresses
    }

    struct Address {
      1: required string street
      2: required string zip
      3: required PhoneNumber primaryPhone
      4: required PhoneNumber secondaryPhone
      4: required list<PhoneNumber> otherPhones
    }

    struct PhoneNumber {
      1: required i32 areaCode
      2: required i32 number
      3: required bool doNotCall
    }

A column is specified as the path from the root of the schema down to the field of interest, separated by `.`, just as you would access the field
in java or scala code. For example: `primaryAddress.primaryPhone.doNotCall`.
This applies for repeated fields as well, for example `primaryAddress.otherPhones.number` selects all the `number`s from all the elements of `otherPhones`.
Maps are a special case -- the map is split into two columns, the key and the value. All the columns in the key are required, but you can select a subset of the
columns in the value (or skip the value entirely), for example: `otherAddresses.{key,value.street}` will select only the streets from the
values of the map, but the entire key will be kept. To select an entire map, you can do: `otherAddresses.{key,value}`, 
and to select only the keys: `otherAddresses.key`. Similar to map keys, the values in a set cannot be partially projected,
you must select all the columns of the items in the set, or none of them. This is because materializing the set wouldn't make much sense if the item's
hashcode is dependent on the dropped columns (as with the key of a map).

When selecting a field that is a struct, for example `primaryAddress.primaryPhone`, 
it will select the entire struct. So `primaryAddress.primaryPhone.*` is redundant.

Columns can be specified concretely (like `primaryAddress.primaryPhone.doNotCall`), or a restricted glob syntax can be used.
The glob syntax supports only wildcards (`*`) and glob expressions (`{}`).

For example:

  * `name` will select just the `name` from the Person
  * `{name,age}` will select both the `name` and `age` from the Person
  * `primaryAddress` will select the entire `primaryAddress` struct, including all of its children (recursively)
  * `primaryAddress.*Phone` will select all of `primaryAddress.primaryPhone` and `primaryAddress.secondaryPhone`
  * `primaryAddress.*Phone*` will select all of `primaryAddress.primaryPhone` and `primaryAddress.secondaryPhone` and `primaryAddress.otherPhones`
  * `{name,age,primaryAddress.{*Phone,street}}` will select `name`, `age`, `primaryAddress.primaryPhone`, `primaryAddress.secondaryPhone`, and `primaryAddress.street`

Multiple Patterns:
Multiple glob expression can be joined together separated by ";". eg. `name;primaryAddress.street` will match only name and street in Address.
This is useful if you want to combine a list of patterns without making a giant `{}` group.

Note: all possible glob patterns must match at least one column. For example, if you provide the glob: `a.b.{c,d,e}` but only columns `a.b.c` and `a.b.d` exist, an
exception will be thrown.

You can provide your projection globs to parquet by setting `parquet.thrift.column.projection.globs` in the hadoop config, or using the methods in the
scheme builder classes.

### 2.2 Projection Pushdown with Tuples
When using ParquetTupleScheme, specifying projection pushdown is as simple as specifying fields as the parameter of the constructor of ParquetTupleScheme:


3. Cascading 2.0 & Cascading 3.0
================================
Cascading 3.0 introduced a breaking interface change in the Scheme abstract class, which causes a breaking change in all scheme implementations.
The parquet-cascading3 directory contains a separate library for use with Cascading 3.0

A significant part of the code remains identical; this shared part is in the parquet-cascading-common23 directory, which is not a Maven module.

You cannot use both parquet-cascading and parquet-cascading3 in the same Classloader, which should be fine as you cannot use both cascading-core 2.x and cascading-core 3.x in the same Classloader either.




`Scheme sourceScheme = new ParquetTupleScheme(new Fields("age"));`
