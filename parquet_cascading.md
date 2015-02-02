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

In [parquet-cascading](http://https://github.com/Parquet/parquet-mr/tree/master/parquet-cascading) sub-module, we provide support for reading/writing records of various data structures including Thrift(TBase), Scrooge and Tuples. Please refer to following sections for each data structures.

1.1 Thrift/TBase
------------
### Read Thrift Records from Parquet
[ParquetTbaseScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/main/java/parquet/cascading/ParquetTBaseScheme.java) is the interface for reading thrift records in Parquet format. Providing a ParquetTbaseScheme as a parameter to the constructor of a source enables the program to read Thrift object(TBase), eg.

`
Scheme sourceScheme = new ParquetTBaseScheme(Name.class)
Tap source = new Hfs(sourceScheme, parquetInputPath);
`

In the above example Name is a thrift class that extends TBase. Under the hood parquet will generate a schema from the thrift class to decode the data. 

The thrift class is actually *optional* to initialize a ParquetTBaseScheme when the data is written as Thrift records in Parquet. When writing thrift records to parquet format, the Thrift class of the records is stored as meta-data in the footer of the parquet file. Therefore when reading the file, if a thrift class is not explicitly provided, Parquet will use the class name stored in the footer as the thrift class. 

### Write Thrift Records to Parquet
[ParquetTbaseScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/main/java/parquet/cascading/ParquetTBaseScheme.java) can also be used by a sink. When used as a sink, the Thrift class of the records being written must be *explicitly* provided.

`
Scheme sinkScheme = new ParquetTBaseScheme(Name.class);
Tap sink = new Hfs(sinkScheme, parquetOutputPath);
`

For more concrete examples please refer to [TestParquetTBaseScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/test/java/parquet/cascading/TestParquetTBaseScheme.java)

1.2 Scrooge
-----------
### Read Scrooge records from Parquet
Scrooge support is defined in a separate module called [parquet-scrooge](https://github.com/Parquet/parquet-mr/tree/master/parquet-scrooge). With [ParquetScroogeScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-scrooge/src/main/java/parquet/scrooge/ParquetScroogeScheme.java), data can be read in the form of Scrooge objects which are more scala friendly.

`
Scheme sinkScheme = new ParquetScroogeScheme(Name.class);
Tap sink = new Hfs(sinkScheme, parquetOutputPath);
`

### Write Scrooge Records to Parquet(Not supported yet)

1.3 Tuples
----------
### Read Cascading Tuples from Parquet
Currently, the support for reading tuples is mainly(but not limited) for data written from pig scripts as pig tuples. More comprehensive support will be added, but in the mean time, there are some limitations to notice: Nested structures are not supported. If the data is written as thrift objects which have nested structure, it can not be read at current time. *Data to read must be in flat structure*. To read data as tuples, simply use [ParquetTupleScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/main/java/parquet/cascading/ParquetTupleScheme.java):

`
Scheme sourceScheme = new ParquetTupleScheme(new Fields("last_name"));
Tap source = new Hfs(sourceScheme, parquetInputPath);
`

### Write Cascading Tuples to Parquet(coming soon)

For more examples please refer to [TestParquetTupleScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/test/java/parquet/cascading/TestParquetTupleScheme.java)

2. Projection Pushdown
======================
One of the big benefit of using columnar format is to be able to read only a subset of columns when the full schema is huge. It saves times by not reading unused columns. 

Parquet support projection pushdown for Thrift records and tuples.

### 2.1 Projection Pushdown with Thrift/Scrooge Records
To read only a subset of attributes in a Thrift/Scrooge class, the columns of interest should be specified using glob syntax. For example, for a thrift class as follows:

    
    struct Address{
      1: string street
      2: string zip
    }
    struct Person{
      1: string name
      2: int16 age
      3: Address addr
    }


In the above example, when reading records of type Person, we can use following glob expression to specify the attributes we want to read:

- Exact match:
"`name`" will only fetch the name attribute.

- Alternative match:
"`address/{street,zip}`" will fetch both street and zip in the Address

- Wildcard match:
"`*`" will fetch name and age, but not address, since address is a nested structure

- Recursive match:
"`**`" will recursively match all attributes defined in Person.

- Joined match:
Multiple glob expression can be joined together separated by ";". eg. "name;address/street" will match only name and street in Address.

To specify the glob filter for thrift/scrooge, simply set the conf with "parquet.thrift.column.filter" set to the glob expression string.


    Map<Object, Object> props=new HashMap<Object, Object>();
    props.put("parquet.thrift.column.filter","name;address/street");
    HadoopFlowConnector hadoopFlowConnector = new HadoopFlowConnector(props);


### 2.2 Projection Pushdown with Tuples
When using ParquetTupleScheme, specifying projection pushdown is as simple as specifying fields as the parameter of the constructor of ParquetTupleScheme:

`Scheme sourceScheme = new ParquetTupleScheme(new Fields("age"));`
