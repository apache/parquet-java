/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.thrift.struct;

import org.apache.thrift.TFieldRequirementType;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class ThriftField {
  public static enum Requirement {
    REQUIRED(TFieldRequirementType.REQUIRED),
    OPTIONAL(TFieldRequirementType.OPTIONAL),
    DEFAULT(TFieldRequirementType.DEFAULT);

    private final byte requirement;

    private Requirement(byte requirement) {
      this.requirement = requirement;
    }

    public byte getRequirement() {
      return requirement;
    }
  }

  private final String name;
  private final short fieldId;
  private final Requirement requirement;
  private final ThriftType type;

  @JsonCreator
  public ThriftField(@JsonProperty("name") String name, @JsonProperty("fieldId") short fieldId, @JsonProperty("requirement") Requirement requirement, @JsonProperty("type") ThriftType type) {
    super();
    this.name = name;
    this.fieldId = fieldId;
    this.requirement = requirement;
    this.type = type;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the fieldId
   */
  public short getFieldId() {
    return fieldId;
  }

  /**
   * @return the type
   */
  public ThriftType getType() {
    return type;
  }

  /**
   * @return the requirement
   */
  public Requirement getRequirement() {
    return requirement;
  }

  @Override
  public String toString() {
    return JSON.toJSON(this);
  }

}
