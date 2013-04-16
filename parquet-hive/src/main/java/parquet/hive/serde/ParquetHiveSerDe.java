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
package parquet.hive.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.hive.writable.BinaryWritable;
/**
 *
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class ParquetHiveSerDe implements SerDe {

    private List<String> columnNames;
    private List<TypeInfo> columnTypes;

    ObjectInspector objInspector;

    static final Log LOG = LogFactory.getLog(MapWritableObjectInspector.class);

    @Override
    final public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

        final TypeInfo rowTypeInfo;

        // Get column names and sort order
        final String columnNameProperty = tbl.getProperty("columns");
        final String columnTypeProperty = tbl.getProperty("columns.types");

        if (columnNameProperty.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }

        if (columnTypeProperty.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        if (columnNames.size() != columnTypes.size()) {
            throw new RuntimeException("ParquetHiveSerde initialization failed. Number of column name and column type differs.");
        }

        // Create row related objects
        rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

        LOG.error(ColumnProjectionUtils.getReadColumnIDs(conf));
        this.objInspector = new MapWritableObjectInspector((StructTypeInfo) rowTypeInfo);
    }

    @Override
    public Object deserialize(final Writable blob) throws SerDeException {
        if (blob instanceof MapWritable) {
            return blob;
        } else {
            return null;
        }
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BytesWritable.class;
    }

    @Override
    public Writable serialize(final Object obj, final ObjectInspector objInspector) throws SerDeException {
        if (!objInspector.getCategory().equals(Category.STRUCT)) {
            throw new SerDeException("Can only serialize a struct");
        }

        return createMapWritableFromRaw(obj, (StructObjectInspector) objInspector, columnNames);
    }

    private MapWritable createMapWritableFromRaw(final Object obj, final StructObjectInspector objInspector, final List<String> colNames) throws SerDeException {
        final MapWritable result = new MapWritable();
        final List<? extends StructField> fields = objInspector.getAllStructFieldRefs();
        int i = 0;

        for (final StructField field : fields) {

            final Object subObj = objInspector.getStructFieldData(obj, field);
            final ObjectInspector inspector = field.getFieldObjectInspector();
            final Category cat = inspector.getCategory();

            Writable subResult = null;
            switch (cat) {
            case STRUCT:
                subResult = createMapWritableFromRaw(subObj, (StructObjectInspector) inspector, null);
                break;
            case LIST:
            case MAP:
                throw new NotImplementedException("Array/map serialization not implemented");
            case PRIMITIVE:
                subResult = createPrimitiveWritableFromRaw(subObj, (PrimitiveObjectInspector) inspector);
                break;
            default:
                throw new RuntimeException("Unknown data type");
            }

            // for the 1st lvl, the field names are "_col0" ... and we want the
            // real names
            final String colName = (colNames != null) ? colNames.get(i++) : field.getFieldName();
            if (subResult != null) {
                result.put(new Text(colName), subResult);
            }
        }

        return result;

    }

    private Writable createPrimitiveWritableFromRaw(final Object obj, final PrimitiveObjectInspector inspector) throws SerDeException {

        if (obj == null) {
            return null;
        }

        switch (inspector.getPrimitiveCategory()) {
        case VOID:
            return null;
        case BOOLEAN:
            return new BooleanWritable(((BooleanObjectInspector) inspector).get(obj) ? Boolean.TRUE : Boolean.FALSE);
        case BYTE:
            return new ByteWritable((byte) ((ByteObjectInspector) inspector).get(obj));
        case DOUBLE:
            return new DoubleWritable(((DoubleObjectInspector) inspector).get(obj));
        case FLOAT:
            return new FloatWritable(((FloatObjectInspector) inspector).get(obj));
        case INT:
            return new IntWritable(((IntObjectInspector) inspector).get(obj));
        case LONG:
            return new LongWritable(((LongObjectInspector) inspector).get(obj));
        case SHORT:
            return new ByteWritable((byte) ((ShortObjectInspector) inspector).get(obj));
        case STRING:
            return new BinaryWritable(((StringObjectInspector) inspector).getPrimitiveJavaObject(obj));
        default:
            throw new SerDeException("Unknown primitive");
        }
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
