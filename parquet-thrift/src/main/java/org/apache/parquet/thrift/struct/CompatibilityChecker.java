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


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
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
import org.apache.parquet.Strings;

/**
 * A checker for thrift struct to enforce its backward compatibility, returns compatibility report based on following rules:
 * 1. Should not add new REQUIRED field in new thrift struct. Adding optional field is OK
 * 2. Should not change field type for an existing field
 * 3. Should not delete existing field
 * 4. Should not make requirement type more restrictive for a field in new thrift struct
 *
 * @author Tianshuo Deng
 */
public class CompatibilityChecker {

  public CompatibilityReport checkCompatibility(ThriftType.StructType oldStruct, ThriftType.StructType newStruct) {
    CompatibleCheckerVisitor visitor = new CompatibleCheckerVisitor(oldStruct);
    newStruct.accept(visitor, new FieldsPath());
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

    return Strings.join(messages, "\n");
  }

  @Override
  public String toString() {
    return "CompatibilityReport{" +
        "isCompatible=" + isCompatible +
        ", hasEmptyStruct=" + hasEmptyStruct +
        ", messages=\n" + prettyMessages() +
        '}';
  }
}

class CompatibleCheckerVisitor implements ThriftType.TypeVisitor<Void, FieldsPath> {


  ThriftType oldType;

  CompatibilityReport report = new CompatibilityReport();

  CompatibleCheckerVisitor(ThriftType.StructType oldType) {
    this.oldType = oldType;
  }

  public CompatibilityReport getReport() {
    return report;
  }

  @Override
  public Void visit(ThriftType.MapType mapType, FieldsPath path) {
    ThriftType.MapType currentOldType = ((ThriftType.MapType) oldType);
    ThriftField oldKeyField = currentOldType.getKey();
    ThriftField newKeyField = mapType.getKey();

    ThriftField newValueField = mapType.getValue();
    ThriftField oldValueField = currentOldType.getValue();

    checkField(oldKeyField, newKeyField, path);
    checkField(oldValueField, newValueField, path);

    oldType = currentOldType;
  }

  @Override
  public Void visit(ThriftType.SetType setType, FieldsPath path) {
    ThriftType.SetType currentOldType = ((ThriftType.SetType) oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = setType.getValues();
    checkField(oldField, newField, path);
    oldType = currentOldType;
  }

  @Override
  public Void visit(ThriftType.ListType listType, FieldsPath path) {
    ThriftType.ListType currentOldType = ((ThriftType.ListType) oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = listType.getValues();
    checkField(oldField, newField, path);
    oldType = currentOldType;
  }

  public void incompatible(String message, FieldsPath path) {
    report.fail("at " + path + ":" +message);
  }

  private void checkField(ThriftField oldField, ThriftField newField, FieldsPath path) {

    if (!newField.getType().getType().equals(oldField.getType().getType())) {
      incompatible("type is not compatible: " + oldField.getType().getType() + " vs " + newField.getType().getType(), path);
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

    oldType = oldField.getType();
    newField.getType().accept(this, path.push(newField));
  }

  private boolean firstIsMoreRestirctive(ThriftField.Requirement firstReq, ThriftField.Requirement secReq) {
    if (firstReq == ThriftField.Requirement.REQUIRED && secReq != ThriftField.Requirement.REQUIRED) {
      return true;
    } else {
      return false;
    }

  }

  @Override
  public Void visit(ThriftType.StructType newStruct, FieldsPath path) {
    ThriftType.StructType currentOldType = ((ThriftType.StructType) oldType);
    short oldMaxId = 0;

    if (newStruct.getChildren().isEmpty()) {
      report.emptyStruct("encountered an empty struct: " + path);
    }

    for (ThriftField oldField : currentOldType.getChildren()) {
      short fieldId = oldField.getFieldId();
      if (fieldId > oldMaxId) {
        oldMaxId = fieldId;
      }
      ThriftField newField = newStruct.getChildById(fieldId);
      if (newField == null) {
        incompatible("can not find index in new Struct: " + fieldId + " in " + newStruct, path);
        return null;
      }
      checkField(oldField, newField, path);
    }

    //check for new added
    for (ThriftField newField : newStruct.getChildren()) {
      //can not add required
      if (newField.getRequirement() != ThriftField.Requirement.REQUIRED)
        continue;//can add optional field

      short newFieldId = newField.getFieldId();
      if (newFieldId > oldMaxId) {
        incompatible("new required field " + newField.getName() + " is added", path);
        return null;
      }
      if (newFieldId < oldMaxId && currentOldType.getChildById(newFieldId) == null) {
        incompatible("new required field " + newField.getName() + " is added", path);
        return null;
      }

    }

    //restore
    oldType = currentOldType;
  }

  @Override
  public Void visit(EnumType enumType, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(BoolType boolType, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(ByteType byteType, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(DoubleType doubleType, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(I16Type i16Type, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(I32Type i32Type, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(I64Type i64Type, FieldsPath path) {
    return null;
  }

  @Override
  public Void visit(StringType stringType, FieldsPath path) {
    return null;
  }
}


