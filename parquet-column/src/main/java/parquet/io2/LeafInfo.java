/**
 * Copyright 2014 GoDaddy, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io2;

final class LeafInfo {
  private final ColumnPath physicalPath;
  private final ColumnPath logicalPath;

  static final LeafInfo empty = new LeafInfo(ColumnPath.empty, ColumnPath.empty);

  LeafInfo(
      final ColumnPath physicalPath,
      final ColumnPath logicalPath) {
    this.physicalPath = physicalPath;
    this.logicalPath = logicalPath;
  }

  LeafInfo combine(final LeafInfo other) {
    return new LeafInfo(
        this.physicalPath.combine(other.physicalPath),
        this.logicalPath.combine(other.logicalPath));
  }

  public ColumnPath getPhysicalPath() {
    return physicalPath;
  }

  public ColumnPath getLogicalPath() {
    return logicalPath;
  }

  @Override
  public String toString() {
    return "LeafInfo{" +
        "physicalPath=" + physicalPath +
        ", logicalPath=" + logicalPath +
        '}';
  }
}
