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
 *
 * @author Tianshuo Deng
 */
public class CompatibilityChecker {

  public CompatibilityReport checkCompatibility(ThriftType.StructType oldStruct, ThriftType.StructType newStruct) {
    CompatibleCheckerVisitor visitor = new CompatibleCheckerVisitor(oldStruct);
    newStruct.accept(visitor);
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
    StringBuffer message = new StringBuffer();
    for(String m: messages) {
      message.append(m);
      message.append("\n");
    }
    return message.toString();
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


class CompatibleCheckerVisitor implements ThriftType.TypeVisitor<Void, Void> {
  /**
   * log the path when a incompatible field is found, so it's easier to track down the incompatible field
   */
  private static class FieldPath {
    Deque<String> path = new ArrayDeque<String>();

    void push(String fieldName) {
      path.push(fieldName);
    }

    String pop() {
      return path.pop();
    }

    @Override
    public String toString() {
      StringBuffer buffer = new StringBuffer("/");
      Iterator<String> it = path.descendingIterator();
      while(it.hasNext()) {
        buffer.append(it.next());
        buffer.append('/');
      }
      return buffer.toString();
    }
  }

  ThriftType oldType;
  FieldPath path = new FieldPath();

  CompatibilityReport report = new CompatibilityReport();

  CompatibleCheckerVisitor(ThriftType.StructType oldType) {
    this.oldType = oldType;
  }

  public CompatibilityReport getReport() {
    return report;
  }

  @Override
  public void visit(ThriftType.MapType mapType) {
    ThriftType.MapType currentOldType = ((ThriftType.MapType) oldType);
    ThriftField oldKeyField = currentOldType.getKey();
    ThriftField newKeyField = mapType.getKey();

    ThriftField newValueField = mapType.getValue();
    ThriftField oldValueField = currentOldType.getValue();

    checkField(oldKeyField, newKeyField);
    checkField(oldValueField, newValueField);

    oldType = currentOldType;
  }

  @Override
  public void visit(ThriftType.SetType setType) {
    ThriftType.SetType currentOldType = ((ThriftType.SetType) oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = setType.getValues();
    checkField(oldField, newField);
    oldType = currentOldType;
  }

  @Override
  public void visit(ThriftType.ListType listType) {
    ThriftType.ListType currentOldType = ((ThriftType.ListType) oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = listType.getValues();
    checkField(oldField, newField);
    oldType = currentOldType;
  }

  public void incompatible(String message) {
    report.fail("at " + path + ":" +message);
  }

  private void checkField(ThriftField oldField, ThriftField newField) {

    if (!newField.getType().getType().equals(oldField.getType().getType())) {
      incompatible("type is not compatible: " + oldField.getType().getType() + " vs " + newField.getType().getType());
      return;
    }

    if (!newField.getName().equals(oldField.getName())) {
      incompatible("field names are different: " + oldField.getName() + " vs " + newField.getName());
      return;
    }

    if (firstIsMoreRestirctive(newField.getRequirement(), oldField.getRequirement())) {
      incompatible("new field is more restrictive: " + newField.getName());
      return;
    }

    oldType = oldField.getType();
    newField.getType().accept(this);
  }

  private boolean firstIsMoreRestirctive(ThriftField.Requirement firstReq, ThriftField.Requirement secReq) {
    if (firstReq == ThriftField.Requirement.REQUIRED && secReq != ThriftField.Requirement.REQUIRED) {
      return true;
    } else {
      return false;
    }

  }

  @Override
  public void visit(ThriftType.StructType newStruct) {
    ThriftType.StructType currentOldType = ((ThriftType.StructType) oldType);
    short oldMaxId = 0;

    if (newStruct.getChildren().size() == 0) {
      report.emptyStruct("new struct is an empty struct: " + path);
    }

    for (ThriftField oldField : currentOldType.getChildren()) {
      short fieldId = oldField.getFieldId();
      if (fieldId > oldMaxId) {
        oldMaxId = fieldId;
      }
      ThriftField newField = newStruct.getChildById(fieldId);
      if (newField == null) {
        incompatible("can not find index in new Struct: " + fieldId + " in " + newStruct);
        return;
      }
      checkField(oldField, newField);
    }

    //check for new added
    for (ThriftField newField : newStruct.getChildren()) {
      //can not add required
      if (newField.getRequirement() != ThriftField.Requirement.REQUIRED)
        continue;//can add optional field

      short newFieldId = newField.getFieldId();
      if (newFieldId > oldMaxId) {
        fail("new required field " + newField.getName() + " is added");
        return;
      }
      if (newFieldId < oldMaxId && currentOldType.getChildById(newFieldId) == null) {
        fail("new required field " + newField.getName() + " is added");
        return;
      }

    }

    //restore
    oldType = currentOldType;
  }

  @Override
  public void visit(EnumType enumType) {
  }

  @Override
  public void visit(BoolType boolType) {
  }

  @Override
  public void visit(ByteType byteType) {
  }

  @Override
  public void visit(DoubleType doubleType) {
  }

  @Override
  public void visit(I16Type i16Type) {
  }

  @Override
  public void visit(I32Type i32Type) {
  }

  @Override
  public void visit(I64Type i64Type) {
  }

  @Override
  public void visit(StringType stringType) {
  }
}


