/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace java org.apache.parquet.thrift.test.compat

struct ListOfInts {
  1: required list<i32> list_of_ints;
}

struct Location {
  1: required double latitude;
  2: required double longitude;
}

struct ListOfLocations {
  1: optional list<Location> locations;
}

struct SingleElementGroup {
  1: required i64 count;
}

struct SingleElementGroupDifferentName {
  1: required i64 differentFieldName;
}

struct ListOfSingleElementGroups {
  1: optional list<SingleElementGroup> single_element_groups;
}

struct ListOfCounts {
  1: optional list<i64> single_element_groups;
}

struct ListOfLists {
  1: optional list<list<i32>> listOfLists;
}
