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
package org.apache.parquet.jackson;

/**
 * Apache Parquet Jackson module placeholder class.
 *
 * <p>This module provides shaded Jackson dependencies for use by other Parquet modules.
 * All Jackson classes are relocated under the {@code parquet.com.fasterxml.jackson.core} namespace.
 *
 * <p>This class exists solely to enable javadoc generation for this module.
 * The actual functionality is provided by shaded Jackson classes.
 *
 * @see <a href="https://github.com/FasterXML/jackson/">Jackson</a>
 */
public final class ParquetJackson {
  private ParquetJackson() {
    // Utility class - prevent instantiation
  }
}
