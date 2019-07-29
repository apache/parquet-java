<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

## Building

You can build this project using maven:

```
mvn clean install -DskipTests
```


## Running

The build produces a shaded Jar that can be run using the `hadoop` command:

```
hadoop jar parquet-cli-1.9.1-runtime.jar org.apache.parquet.cli.Main
```

For a shorter command-line invocation, add an alias to your shell like this:

```
alias parquet="hadoop jar /path/to/parquet-cli-1.9.1-runtime.jar org.apache.parquet.cli.Main --dollar-zero parquet"
```

### Running without Hadoop

To run from the target directory instead of using the `hadoop` command, first copy the dependencies to a folder:

```
mvn dependency:copy-dependencies
```

Then, run the command-line and add `target/dependencies/*` to the classpath:

```
java -cp 'target/*:target/dependency/*' org.apache.parquet.cli.Main
```


### Help

The `parquet` tool includes help for the included commands:

```
parquet help
```
```
Usage: parquet [options] [command] [command options]

  Options:

    -v, --verbose, --debug
        Print extra debugging information

  Commands:

    help
        Retrieves details on the functions of other commands
    meta
        Print a Parquet file's metadata
    pages
        Print page summaries for a Parquet file
    dictionary
        Print dictionaries for a Parquet column
    check-stats
        Check Parquet files for corrupt page and column stats (PARQUET-251)
    schema
        Print the Avro schema for a file
    csv-schema
        Build a schema from a CSV data sample
    convert-csv
        Create a file from CSV data
    convert
        Create a Parquet file from a data file
    to-avro
        Create an Avro file from a data file
    cat
        Print the first N records from a file
    head
        Print the first N records from a file

  Examples:

    # print information for create
    parquet help create

  See 'parquet help <command>' for more information on a specific command.
```

