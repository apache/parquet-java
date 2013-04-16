package parquet.hive.serde;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class ParquetHiveArrayInspector implements ListObjectInspector {

    ObjectInspector arrayElementInspector;

    public ParquetHiveArrayInspector(final ObjectInspector arrayElementInspector) {
        this.arrayElementInspector = arrayElementInspector;
    }

    @Override
    public String getTypeName() {
        return "ParquetHiveArray<" + arrayElementInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
        return Category.LIST;
    }

    @Override
    public ObjectInspector getListElementObjectInspector() {
        return arrayElementInspector;
    }

    @Override
    public Object getListElement(final Object data, final int index) {
        if (data == null) {
            return null;
        }

        final Writable subObj = ((MapWritable) data).get(ParquetHiveSerDe.ARRAY);

        if (subObj == null) {
            return null;
        }

        return ((ArrayWritable) subObj).get()[index];
    }

    @Override
    public int getListLength(final Object data) {
        if (data == null) {
            return 0;
        }

        final Writable subObj = ((MapWritable) data).get(ParquetHiveSerDe.ARRAY);

        if (subObj == null) {
            return 0;
        }

        return ((ArrayWritable) subObj).get().length;
    }

    @Override
    public List<?> getList(final Object data) {
        if (data == null) {
            return null;
        }

        final Writable subObj = ((MapWritable) data).get(ParquetHiveSerDe.ARRAY);

        if (subObj == null) {
            return null;
        }

        final Writable[] array = ((ArrayWritable) subObj).get();
        final List<Writable> list = new ArrayList<Writable>();

        for (final Writable obj : array) {
            list.add(obj);
        }

        return list;
    }

}
