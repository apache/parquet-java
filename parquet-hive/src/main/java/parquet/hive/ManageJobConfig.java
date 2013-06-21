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
package parquet.hive;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * ManageJobConfig quick workaround to initialize the job with column needed
 *
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class ManageJobConfig {

  MapredWork mrwork;
  private Map<String, PartitionDesc> pathToPartitionInfo;

  public static List<String> getColumns(final String columns) {
    final List<String> listColumns = (List<String>) StringUtils.getStringCollection(columns);
    // Remove virtual Hive columns, hardcoded for compatibility
    listColumns.remove("INPUT__FILE__NAME");
    listColumns.remove("BLOCK__OFFSET__INSIDE__FILE");
    listColumns.remove("ROW__OFFSET__INSIDE__BLOCK");
    listColumns.remove("RAW__DATA__SIZE");
    return listColumns;
  }

  private void init(final JobConf job) {
    final String plan = HiveConf.getVar(job, HiveConf.ConfVars.PLAN);
    if (mrwork == null && plan != null && plan.length() > 0) {
      mrwork = Utilities.getMapRedWork(job);
      pathToPartitionInfo = new LinkedHashMap<String, PartitionDesc>();
      for (final Map.Entry<String, PartitionDesc> entry : mrwork.getPathToPartitionInfo().entrySet()) {
        pathToPartitionInfo.put(new Path(entry.getKey()).toUri().getPath().toString(), entry.getValue());
      }
    }
  }

  private PartitionDesc getPartition(final String path) {
    if (pathToPartitionInfo == null) {
      return null;
    }
    return pathToPartitionInfo.get(path);
  }

  private void pushProjectionsAndFilters(final JobConf jobConf, final String splitPath, final String splitPathWithNoSchema) {

    if (mrwork == null || mrwork.getPathToAliases() == null) {
      return;
    }

    final ArrayList<String> aliases = new ArrayList<String>();
    final Iterator<Entry<String, ArrayList<String>>> iterator = this.mrwork.getPathToAliases().entrySet().iterator();

    while (iterator.hasNext()) {
      final Entry<String, ArrayList<String>> entry = iterator.next();
      final String key = new Path(entry.getKey()).toUri().getPath();

      if (splitPath.equals(key) || splitPathWithNoSchema.equals(key)) {
        final ArrayList<String> list = entry.getValue();
        for (final String val : list) {
          aliases.add(val);
        }
      }
    }

    for (final String alias : aliases) {
      final Operator<? extends Serializable> op = this.mrwork.getAliasToWork().get(
          alias);
      if (op != null && op instanceof TableScanOperator) {
        final TableScanOperator tableScan = (TableScanOperator) op;

        // push down projections
        final ArrayList<Integer> list = tableScan.getNeededColumnIDs();

        if (list != null) {
          ColumnProjectionUtils.appendReadColumnIDs(jobConf, list);
        } else {
          ColumnProjectionUtils.setFullyReadColumns(jobConf);
        }

        pushFilters(jobConf, tableScan);
      }
    }
  }

  private void pushFilters(final JobConf jobConf, final TableScanOperator tableScan) {

    final TableScanDesc scanDesc = tableScan.getConf();
    if (scanDesc == null) {
      return;
    }

    // construct column name list for reference by filter push down
    Utilities.setColumnNameList(jobConf, tableScan);

    // push down filters
    final ExprNodeDesc filterExpr = scanDesc.getFilterExpr();
    if (filterExpr == null) {
      return;
    }

    final String filterText = filterExpr.getExprString();
    final String filterExprSerialized = Utilities.serializeExpression(filterExpr);
    jobConf.set(
        TableScanDesc.FILTER_TEXT_CONF_STR,
        filterText);
    jobConf.set(
        TableScanDesc.FILTER_EXPR_CONF_STR,
        filterExprSerialized);
  }

  public JobConf cloneJobAndInit(final JobConf jobConf, final Path path) throws IOException {
    init(jobConf);
    final JobConf cloneJobConf = new JobConf(jobConf);
    final PartitionDesc part = getPartition(path.toString());

    if ((part != null) && (part.getTableDesc() != null)) {
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), cloneJobConf);
    }

    pushProjectionsAndFilters(cloneJobConf, path.toString(), path.toUri().toString());
    return cloneJobConf;
  }
}
