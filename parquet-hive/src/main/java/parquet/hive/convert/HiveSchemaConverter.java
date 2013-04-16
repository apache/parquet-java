/**
 * Copyright 2013 Criteo.
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
package parquet.hive.convert;

import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

/**
 *
 * A HiveSchemaConverter
 *
 *
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class HiveSchemaConverter {
    static public MessageType convert(final List<String> columnNames, final List<TypeInfo> columnTypes) {
        return new MessageType("hive_schema", convertTypes(columnNames, columnTypes));
    }

    static private Type[] convertTypes(final List<String> columnNames, final List<TypeInfo> columnTypes) {
        if (columnNames.size() != columnTypes.size()) {
            throw new RuntimeException("Mismatched Hive columns and types");
        }

        final Type[] types = new Type[columnNames.size()];

        for (int i = 0; i < columnNames.size(); ++i) {
            types[i] = convertType(columnNames.get(i), columnTypes.get(i));
        }

        return types;
    }

    static private Type convertType(final String name, final TypeInfo typeInfo) {
        if (typeInfo.getCategory().equals(Category.PRIMITIVE)) {
            if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
                return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, name);
            } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo) || typeInfo.equals(TypeInfoFactory.shortTypeInfo) || typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
                return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, name);
            } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
                return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, name);
            } else if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
                return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, name);
            } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
                return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, name);
            } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
                return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, name);
            } else {
                throw new RuntimeException();
            }
        } else if (typeInfo.getCategory().equals(Category.LIST)) {
            throw new NotImplementedException("Array hive conversion not implemented");
        } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
            throw new NotImplementedException("Struct hive conversion not implemented");
        } else if (typeInfo.getCategory().equals(Category.MAP)) {
            throw new NotImplementedException("Map hive conversion not implemented");
        } else {
            throw new RuntimeException("Unknown type: " + typeInfo);
        }
    }
}
