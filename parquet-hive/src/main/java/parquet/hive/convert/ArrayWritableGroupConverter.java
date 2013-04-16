package parquet.hive.convert;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import parquet.Log;
import parquet.io.api.Converter;
import parquet.schema.GroupType;

public class ArrayWritableGroupConverter extends HiveGroupConverter {
    private static final Log LOG = Log.getLog(ArrayWritableGroupConverter.class);

    private final GroupType groupType;
    private final Converter[] converters;
    private final HiveGroupConverter parent;
    private final int index;
    private final List<Writable> currentList;
    private ArrayWritable arrayWritable = null;

    public ArrayWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent, final int index) {
        this.parent = parent;
        this.index = index;
        this.groupType = groupType;
        this.currentList = new ArrayList<Writable>();

        if (groupType.getFieldCount() == 2) {
            final MapWritableGroupConverter intermediateConverter = new MapWritableGroupConverter(groupType, this, 0);
            converters = new Converter[groupType.getFieldCount()];
            converters[0] = getConverterFromDescription(groupType.getType(0), 0, intermediateConverter);
            converters[1] = getConverterFromDescription(groupType.getType(1), 1, intermediateConverter);
        } else if (groupType.getFieldCount() == 1) {
            converters = new Converter[1];
            converters[0] = getConverterFromDescription(groupType.getType(0), 0, this);
        } else {
            throw new RuntimeException("Invalid parquet hive schema: " + groupType);
        }

    }

    final public ArrayWritable getCurrentArray() {
        if (arrayWritable == null) {
            arrayWritable = new ArrayWritable(Writable.class);
        }
        arrayWritable.set(currentList.toArray(new Writable[currentList.size()]));
        LOG.info("current array: ");
        for (final Writable a : currentList) {
            if (a instanceof MapWritable) {
                LOG.info("        " + ((MapWritable) a).entrySet());
            } else {
                LOG.info("        " +  a);
            }
        }
        return arrayWritable;
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        LOG.info("starting for array: " + groupType.getName());
        currentList.clear();
    }

    @Override
    public void end() {
        LOG.info("ending for array: " + groupType.getName());
        parent.set(index, getCurrentArray());
    }

    @Override
    protected void set(final int index, final Writable value) {
        LOG.info("current group name: " + groupType.getName());
        LOG.info("setting " + value + " at index " + index + " in list " + currentList);
        if (index != 0) {
            throw new RuntimeException("weee" + index);
        }
        currentList.add(value);
    }

}
