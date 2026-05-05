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
package org.apache.parquet.thrift.struct;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.thrift.projection.FieldsPath;
import org.apache.parquet.thrift.struct.ThriftType.BoolType;
import org.apache.parquet.thrift.struct.ThriftType.ByteType;
import org.apache.parquet.thrift.struct.ThriftType.DoubleType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.I16Type;
import org.apache.parquet.thrift.struct.ThriftType.I32Type;
import org.apache.parquet.thrift.struct.ThriftType.I64Type;
import org.apache.parquet.thrift.struct.ThriftType.StringType;

/**
 * A checker for thrift struct to enforce its backward compatibility, returns compatibility report based on following rules:
 * 1. Should not add new REQUIRED field in new thrift struct. Adding optional field is OK
 * 2. Should not change field type for an existing field
 * 3. Should not delete existing field
 * 4. Should not make requirement type more restrictive for a field in new thrift struct
 */
public class CompatibilityChecker {

  public CompatibilityReport checkCompatibility(ThriftType.StructType oldStruct, ThriftType.StructType newStruct) {
    CompatibleCheckerVisitor visitor = new CompatibleCheckerVisitor();
    newStruct.accept(visitor, new State(oldStruct, new FieldsPath()));
    return visitor.getReport();
  }
}

class CompatibilityReport {
  boolean isCompatible = true;
  boolean hasEmptyStruct = false;
  List<String> messages = new ArrayList<String>();

  public boolean isCompatible() {
    return isCompatible;
  }

  public boolean hasEmptyStruct() {
    return hasEmptyStruct;
  }

  public void fail(String message) {
    messages.add(message);
    isCompatible = false;
  }

  public void emptyStruct(String message) {
    messages.add(message);
    hasEmptyStruct = true;
  }

  public List<String> getMessages() {
    return messages;
  }

  public String prettyMessages() {
    return String.join("\n", messages);
  }

  @Override
  public String toString() {
    return "CompatibilityReport{" + "isCompatible="
        + isCompatible + ", hasEmptyStruct="
        + hasEmptyStruct + ", messages=\n"
        + prettyMessages() + '}';
  }
}

class State {
  FieldsPath path;
  ThriftType oldType;

  public State(ThriftType oldType, FieldsPath fieldsPath) {
    this.path = fieldsPath;
    this.oldType = oldType;
  }
}

class CompatibleCheckerVisitor implements ThriftType.StateVisitor<Void, State> {

  CompatibilityReport report = new CompatibilityReport();

  public CompatibilityReport getReport() {
    return report;
  }

  @Override
  public Void visit(ThriftType.MapType mapType, State state) {
    ThriftType.MapType oldMapType = ((ThriftType.MapType) state.oldType);
    ThriftField oldKeyField = oldMapType.getKey();
    ThriftField newKeyField = mapType.getKey();

    ThriftField newValueField = mapType.getValue();
    ThriftField oldValueField = oldMapType.getValue();

    checkField(oldKeyField, newKeyField, state.path);
    checkField(oldValueField, newValueField, state.path);

    return null;
  }

  @Override
  public Void visit(ThriftType.SetType setType, State state) {
    ThriftType.SetType oldSetType = ((ThriftType.SetType) state.oldType);
    ThriftField oldField = oldSetType.getValues();
    ThriftField newField = setType.getValues();
    checkField(oldField, newField, state.path);
    return null;
  }

  @Override
  public Void visit(ThriftType.ListType listType, State state) {
    ThriftType.ListType currentOldType = ((ThriftType.ListType) state.oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = listType.getValues();
    checkField(oldField, newField, state.path);
    return null;
  }

  public void incompatible(String message, FieldsPath path) {
    report.fail("at " + path + ":" + message);
  }

  private void checkField(ThriftField oldField, ThriftField newField, FieldsPath path) {

    if (!newField.getType().getType().equals(oldField.getType().getType())) {
      incompatible(
          "type is not compatible: " + oldField.getType().getType() + " vs "
              + newField.getType().getType(),
          path);
      return;
    }

    if (!newField.getName().equals(oldField.getName())) {
      incompatible("field names are different: " + oldField.getName() + " vs " + newField.getName(), path);
      return;
    }

    if (firstIsMoreRestirctive(newField.getRequirement(), oldField.getRequirement())) {
      incompatible("new field is more restrictive: " + newField.getName(), path);
      return;
    }

    newField.getType().accept(this, new State(oldField.getType(), path.push(newField)));
  }

  private boolean firstIsMoreRestirctive(ThriftField.Requirement firstReq, ThriftField.Requirement secReq) {
    if (firstReq == ThriftField.Requirement.REQUIRED && secReq != ThriftField.Requirement.REQUIRED) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Void visit(ThriftType.StructType newStruct, State state) {
    ThriftType.StructType oldStructType = ((ThriftType.StructType) state.oldType);
    short oldMaxId = 0;

    if (newStruct.getChildren().isEmpty()) {
      report.emptyStruct("encountered an empty struct: " + state.path);
    }

    for (ThriftField oldField : oldStructType.getChildren()) {
      short fieldId = oldField.getFieldId();
      if (fieldId > oldMaxId) {
        oldMaxId = fieldId;
      }
      ThriftField newField = newStruct.getChildById(fieldId);
      if (newField == null) {
        incompatible("can not find index in new Struct: " + fieldId + " in " + newStruct, state.path);
        return null;
      }
      checkField(oldField, newField, state.path);
    }

    // check for new added
    for (ThriftField newField : newStruct.getChildren()) {
      // can not add required
      if (newField.getRequirement() != ThriftField.Requirement.REQUIRED) continue; // can add optional field

      short newFieldId = newField.getFieldId();
      if (newFieldId > oldMaxId) {
        incompatible("new required field " + newField.getName() + " is added", state.path);
        return null;
      }
      if (newFieldId < oldMaxId && oldStructType.getChildById(newFieldId) == null) {
        incompatible("new required field " + newField.getName() + " is added", state.path);
        return null;
      }
    }

    return null;
  }

  @Override
  public Void visit(EnumType enumType, State state) {
    return null;
  }

  @Override
  public Void visit(BoolType boolType, State state) {
    return null;
  }

  @Override
  public Void visit(ByteType byteType, State state) {
    return null;
  }

  @Override
  public Void visit(DoubleType doubleType, State state) {
    return null;
  }

  @Override
  public Void visit(I16Type i16Type, State state) {
    return null;
  }

  @Override
  public Void visit(I32Type i32Type, State state) {
    return null;
  }

  @Override
  public Void visit(I64Type i64Type, State state) {
    return null;
  }

  @Override
  public Void visit(StringType stringType, State state) {
    return null;
  }

  @Override
  public Void visit(ThriftType.UUIDType uuidType, State state) {
    return null;
  }
}
