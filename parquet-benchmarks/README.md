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
  
##Running Parquet Benchmarks

First, build the ``parquet-benchmarks`` module

```
mvn --projects parquet-benchmarks -amd -DskipTests -Denforcer.skip=true clean package
```

Then, you can run all the benchmarks with the following command

```
./parquet-benchmarks/run.sh -wi 5 -i 5 -f 3 
```

To understand what each command line argument means and for more arguments please see

```
java -jar parquet-benchmarks/target/parquet-benchmarks.jar -help
```
