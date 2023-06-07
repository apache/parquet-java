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
package org.apache.parquet.proto;

/**
 * Constants.
 */
public final class ProtoConstants {

  public static final String METADATA_ENUM_PREFIX = "parquet.proto.enum.";
  public static final String METADATA_ENUM_KEY_VALUE_SEPARATOR = ":";
  public static final String METADATA_ENUM_ITEM_SEPARATOR = ",";
  public static final String IGNORE_UNKNOWN_FIELDS = "IGNORE_UNKNOWN_FIELDS";
  /**
   * Configuration flag to enable reader to accept enum label that's neither defined in its own proto schema nor conform
   * to the "UNKNOWN_ENUM_*" pattern with which we can get the enum number. The enum value will be treated as an unknown
   * enum with number -1. <br>
   * Enabling it will avoid a job failure, but you should perhaps use an up-to-date schema instead.
   */
  public static final String CONFIG_ACCEPT_UNKNOWN_ENUM = "parquet.proto.accept.unknown.enum";

  private ProtoConstants() {
    // Do not instantiate.
  }
}
