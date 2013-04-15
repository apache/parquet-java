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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import parquet.hive.serde.ParquetHiveSerDe;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
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
        return convertType(name, typeInfo, Repetition.OPTIONAL);
    }

    static private Type convertType(String name, TypeInfo typeInfo, Repetition repetition) {
        if (typeInfo.getCategory().equals(Category.PRIMITIVE)) {
            if (typeInfo.equals(TypeInfoFactory.stringTypeInfo))
                return new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name);
            else if (typeInfo.equals(TypeInfoFactory.intTypeInfo) || typeInfo.equals(TypeInfoFactory.shortTypeInfo) || typeInfo.equals(TypeInfoFactory.byteTypeInfo))
                return new PrimitiveType(repetition, PrimitiveTypeName.INT32, name);
            else if (typeInfo.equals(TypeInfoFactory.longTypeInfo))
                return new PrimitiveType(repetition, PrimitiveTypeName.INT64, name);
            else if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo))
                return new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, name);
            else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo))
                return new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, name);
            else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo))
                return new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, name);
            else
                throw new RuntimeException();
            }
        } else if (typeInfo.getCategory().equals(Category.LIST)) {
            return convertArrayType(name, (ListTypeInfo) typeInfo);
        } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
            return convertStructType(name, (StructTypeInfo) typeInfo);
        } else if (typeInfo.getCategory().equals(Category.MAP)) {
            throw new NotImplementedException("Map hive conversion not implemented");
        } else {
            return convertMapType(name, (MapTypeInfo) typeInfo);
        } else
            throw new RuntimeException("Unknown type: " + typeInfo);
        }
    }

    // An optional group containing a repeated anonymous group "bag", containing
    // 1 anonymous element "array_element"
    static private GroupType convertArrayType(String name, ListTypeInfo typeInfo) {
        TypeInfo subType = typeInfo.getListElementTypeInfo();
        return listWrapper(name, OriginalType.LIST, new GroupType(Repetition.REPEATED, ParquetHiveSerDe.ARRAY.toString(), convertType("array_element", subType)));
    }

    // An optional group containing multiple elements
    static private GroupType convertStructType(String name, StructTypeInfo typeInfo) {
        List<String> columnNames = typeInfo.getAllStructFieldNames();
        List<TypeInfo> columnTypes = typeInfo.getAllStructFieldTypeInfos();
        return new GroupType(Repetition.OPTIONAL, name, convertTypes(columnNames, columnTypes));

    }

    // An optional group containing a repeated anonymous group "map", containing
    // 2 elements: "key", "value"
    static private GroupType convertMapType(String name, MapTypeInfo typeInfo) {
        Type keyType = convertType(ParquetHiveSerDe.MAP_KEY.toString(), typeInfo.getMapKeyTypeInfo(), Repetition.REQUIRED);
        Type valueType = convertType(ParquetHiveSerDe.MAP_VALUE.toString(), typeInfo.getMapValueTypeInfo());
        return listWrapper(name, OriginalType.MAP_KEY_VALUE, new GroupType(Repetition.REPEATED, ParquetHiveSerDe.MAP.toString(), keyType, valueType));
    }

    static private GroupType listWrapper(String name, OriginalType originalType, GroupType groupType) {
        return new GroupType(Repetition.OPTIONAL, name, originalType, groupType);
    }
}
