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
package org.apache.parquet.variant;

/**
 * Interface for all converters in a shredded Variant schema. The `init` method is called by the top-level converter
 * after creating a tree of converters. Each converter is responsible for calling `init` on its child converters,
 * either with the same converter, or a new one in the case of object and array typed_values.
 */
interface VariantConverter {
  /**
   * Returns the converter's VariantBuilder, or null if one has not yet been created.
   */
  VariantBuilder getBuilder();
  /*
   * Returns the current converter's parent converter, or null for the top-level converter.
   */
  VariantConverter getParent();
}
