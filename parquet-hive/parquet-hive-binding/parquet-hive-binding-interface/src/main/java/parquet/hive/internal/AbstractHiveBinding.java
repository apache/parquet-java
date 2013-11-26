package parquet.hive.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

import parquet.hive.HiveBinding;

public abstract class AbstractHiveBinding implements HiveBinding {
  private static final List<String> virtualColumns;
  
  static {
    List<String> vcols =  new ArrayList<String>();
    vcols.add("INPUT__FILE__NAME");
    vcols.add("BLOCK__OFFSET__INSIDE__FILE");
    vcols.add("ROW__OFFSET__INSIDE__BLOCK");
    vcols.add("RAW__DATA__SIZE");
    virtualColumns = Collections.unmodifiableList(vcols);
  }
  
  
  @Override
  public List<String> removeVirtualColumns(final String columns) {
    final List<String> result = (List<String>) StringUtils.getStringCollection(columns);
    result.removeAll(virtualColumns);
    return result;
  }

}
