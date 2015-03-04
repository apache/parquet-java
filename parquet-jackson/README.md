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

Parquet Jackson 
======

Parquet-Jackson is just a dummy module to shade [Jackson](http://jackson.codehaus.org/) artifacts.

## Rationale

Parquet internally uses the well-known JSON processor Jackson. Because Apache Hadoop (amongst others) sometimes uses an older version of Jackson, Parquet "shades" its copy of Jackson to prevent any side-effect. 
Originally a copy of Jackson was embedded in each Parquet artifact requiring Jackson, but to prevent duplication, a shared module "Parquet-Jackson" has been created.

Note that this is not a fork of Jackson but the same classes as provided by Jackson artifacts, relocated under the *parquet.org.codehaus.jackson* namespace.

## Detailed explanations

Shading is performed by the [Apache Maven Shade plugin](http://maven.apache.org/plugins/maven-shade-plugin/index.html). It is done during the *package* lifecycle phase, right after the original jar creation. The plugin will replace both the jar and the pom files with new versions with specified dependencies embeded and pom.xml file updated to not refer to those dependencies.

parquet-jackson module will create a new jar artifact containing all Jackson classes, relocated under *parquet.org.codehaus.jackson* package. The shade plugin will transform *pom.xml* too to remove any reference to Jackson dependency.

Other Parquet modules which requires Jackson are configured to depend on parquet-jackson module and still perform shading. The difference is that all Jackson classes are excluded from inclusion, but references from Parquet to Jackson classes are still relocated. The shade plugin will also remove any reference to Jackson dependency but will preserve the parquet-jackson dependency which contains the relocated classes.

### Why still refering directly to org.codehaus.jackson:\* in Parquet modules
Because of the way Maven handles multi-modules project. Let's assume that parquet-foo module uses Jackson. When executing *mvn package*, parquet-jackson module will be built first and artifact will be packaged, and a new pom.xml without Jackson dependency is created and used by parquet-foo module. Since Jackson dependencies have been removed by the shade plugin, compilation of parquet-foo will fail.

