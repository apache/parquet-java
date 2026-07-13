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

# Security

## Reporting a Vulnerability

Please report suspected vulnerabilities privately to the
ASF security team at security@apache.org (they will triage and coordinate the Parquet PMC). 
Do **not** file a public GitHub issue or pull
request for a suspected vulnerability, as that would disclose it before a fix is available.

When reporting, please include as much of the following as you can:
 * a description of the vulnerability and its potential impact;
 * the affected version(s) or commit;
 * steps to reproduce, and a proof of concept if available.

You should receive an acknowledgement from the security team. Please follow the Apache
Software Foundation's vulnerability handling process, described at
https://www.apache.org/security/, and do not publicly disclose the issue until it has been
resolved and an advisory has been published.

## Threat model

General assumptions about the usage of the Parquet library:

* Read configuration (e.g. Hadoop configuration) is controlled by applications. If an application allows untrusted parties to set the read configuration arbitrarily, that is a security risk independent of whether a Parquet file is involved.
* Parquet files (their data and metadata) may originate from untrusted sources. The library parses them on a best-effort basis, but a maliciously crafted file can still cause excessive resource consumption (e.g. large memory allocations or decompression bombs). Applications reading untrusted files are responsible for imposing appropriate resource limits (memory, time, input size) around the library. Where appropriate, improvements to resource management in the library are welcome.

### Class loading

Several modules (e.g. parquet-avro, parquet-protobuf, parquet-thrift, etc.) provide functionality to translate Parquet data into Java objects. In the general case, this requires loading classes named in metadata stored in the Parquet files.

As a consequence:
 * When using these translation layers it is the user's responsibility to make sure their classpath is secure and does not contain any classes that when loaded might cause an adverse issue for their system.
 * If class loading driven by file contents is problematic, end users should use APIs to directly read the data from Parquet instead of relying on translation.
 * Arbitrary class loading by itself is not considered a security issue by the library. A security issue must demonstrate further ability to execute
   arbitrary methods on the instantiated object.
