<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# VECTOR reference files

These files use the draft [VECTOR proposal](https://github.com/apache/parquet-format/pull/624)
at revision `3573b3e1357f6546f3710d8cb48103e61ad40350`. They are
parquet-java-generated examples, not independent interoperability evidence. Runtime
mixed-file tests exercise the writer; these fixed bytes exercise reader compatibility.
The valid files are intended for a later parquet-testing/data export. Malformed 4/6,
content, and count cases and Java exception policy are excluded from portable conformance.

The current `VectorInteropFileGenerator` source blob is
`f5217bb32e86abfac965a3eebab9709adbaa549d`. The three valid files came from
the Round 2 generator source `2d496dad2213216ca6e75f1ab0b4249822829421`.
The malformed file came from the current source. Both used Java 17, Maven 3.9.16,
and Thrift 0.25.0.

## Mixed files

For each row `i=0..5`, `id=i`, `f=[i+0.5f,-0.0f,Float.MIN_VALUE,-0.25f,100.0f]`,
`o` is absent for even `i` and `[7,8]` for odd `i`, and `a` is absent for `i=1,4`.
When present, `a.h=[0080,0100]` in hex. There are `i%3` `outer` instances,
each with `d=[-0.0d,Double.MIN_VALUE]`. Every row has `b=[true,false]`,
`l=[9L]`, `s=[-7]`, `u=[9L]`, `dec=[125]` (logical 1.25), and
`bytes=[00007c00]` in hex (logical 317.44).

## Split file

The two rows are `[0f,1f,2f,3f,4f]` and `[5f,6f,7f,8f,9f]`. Their stored RLs
are `[0,1,1,1,1,0,1,1,1,1]`, with DL=1 throughout. Three unindexed V1 pages
contain 3/4/3 values. The first and last pages use RLE/RLE levels; the middle
uses BIT_PACKED/BIT_PACKED levels. Values are PLAIN, compression is UNCOMPRESSED,
and column and offset indexes are absent.

## Malformed file

`vector_invalid_v1_dictionary.parquet` is deliberately invalid VECTOR data. Its
ordinary V1 writer used a canonical LIST schema with `id=0..3`, then changed only
the footer to annotate eight LIST groups as VECTOR. `short_v` has lengths
`[4,6,5,5]`, `long_v` has `[6,4,5,5]`, `empty_v` has `[0,5,5,5]`, nested `v`
has per-row instance lengths `[[4,6],[],[5],[5]]`, and partial `v` has
`[[5],[5],[5],[3]]`. These groups declare VECTOR(5). Their finite element `k`
in instance `j` of row `i` is `100*i+10*j+k` as FLOAT.

`nf_f`, `nf_d`, and `nf_h` declare VECTOR(8). Each row contains seven ones;
element zero is zero except in row zero, where the fields contain FLOAT NaN,
DOUBLE positive infinity, and FLOAT16 negative infinity respectively. The FLOAT16
values use little-endian bytes `00fc`, `0000`, and `003c` for negative infinity,
zero, and one. The file is UNCOMPRESSED with a page row limit of two. `nf_f` and
`nf_d` each have a dictionary and two V1 PLAIN_DICTIONARY pages of 16 values;
`nf_h` has no dictionary and two V1 PLAIN pages of 16 values. All column-chunk
null counts are unset, and column and offset index references are absent.
Each projected malformed VECTOR field must be rejected.

## SHA-256

```text
c0e182b7f019d394a26bf863626cd58fed3916d930195ab0741fe15b41afb6e8  vector_mixed_v1_plain.parquet
939ce11a1cbe84c68d867deeab85546162d04cab8fd191d5a8efe203bfbb0765  vector_mixed_v2_dictionary.parquet
a598252a6f5715742bdb0e52c86be992f609189096e1cdc518701f4520f4c657  vector_split_v1.parquet
a87042f46c3d56bb9b236795c9c44f13bd02eecb4107040d6bb4b245a73dd7fd  vector_invalid_v1_dictionary.parquet
```

## Reproduce

From the repository root, use a new empty `target/vector-reference` directory.
An occupied directory fails by design; the generator does not overwrite files.
The pinned source and toolchain reproduce the documented rows and layout; future
writer changes need not produce byte-identical files.

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/div.arora/.m2/wrapper/dists/apache-maven-3.9.16/c51baac8/bin/mvn -Dthrift.executable=/home/div.arora/parquet-java-worktrees/vector/target/vector-tools/bin/thrift -pl parquet-hadoop -am -DskipTests package dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=target/vector-test-classpath.txt -Dmdep.regenerateFile=true
/usr/lib/jvm/java-17-openjdk-amd64/bin/java -cp "parquet-hadoop/target/test-classes:parquet-hadoop/target/classes:$(cat parquet-hadoop/target/vector-test-classpath.txt)" org.apache.parquet.hadoop.VectorInteropFileGenerator target/vector-reference
```
