/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Drops specific japicmp compatibility changes from specific classes, for false positives that the
// plugin's <excludes> could only suppress by skipping the whole class. japicmp ignores the
// returned list, so the changes are removed in place.
def ignoredChanges = [
  // GH-3767: PageReader now extends AutoCloseable. japicmp reports the abstract
  // AutoCloseable#close() as added, but PageReader overrides it with a default method, so
  // existing implementations stay source and binary compatible.
  'org.apache.parquet.column.page.PageReader': ['METHOD_ABSTRACT_ADDED_IN_IMPLEMENTED_INTERFACE'],
]

jApiClasses.each { jApiClass ->
  def ignored = ignoredChanges[jApiClass.fullyQualifiedName]
  if (ignored) {
    jApiClass.compatibilityChanges.removeIf { ignored.contains(it.type.name()) }
  }
}

return jApiClasses
