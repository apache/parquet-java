package parquet.hive;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;

public class ManageJobConfig {
    MapredWork mrwork;
    private Map<String, PartitionDesc> pathToPartitionInfo;
    static final Log LOG = LogFactory.getLog(ManageJobConfig.class);

    private void init(final JobConf job) {
        if (mrwork == null) {
            mrwork = Utilities.getMapRedWork(job);

            pathToPartitionInfo = new LinkedHashMap<String, PartitionDesc>();

            for (final Map.Entry<String, PartitionDesc> entry : mrwork.getPathToPartitionInfo().entrySet()) {
                LOG.error("put into Partition DEsc " + new Path(entry.getKey()).toUri().getPath().toString());
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

    private void pushProjectionsAndFilters(final JobConf jobConf,
            final String splitPath, final String splitPathWithNoSchema, final boolean nonNative) {

        init(jobConf);

        LOG.error("this.mrwork.getPathToAliases() :  " + this.mrwork.getPathToAliases() );
        if(this.mrwork.getPathToAliases() == null) {
            return;
        }

        final ArrayList<String> aliases = new ArrayList<String>();
        final Iterator<Entry<String, ArrayList<String>>> iterator = this.mrwork.getPathToAliases().entrySet().iterator();

        while (iterator.hasNext()) {
            final Entry<String, ArrayList<String>> entry = iterator.next();
            final String key = new Path(entry.getKey()).toUri().getPath().toString();
            boolean match;
            LOG.error("non native :  " + nonNative);
            LOG.error("key " + key);
            LOG.error("splitPath" +   splitPath + " splutwithnoshcema " + splitPathWithNoSchema);

            if (nonNative) {
                // For non-native tables, we need to do an exact match to avoid
                // HIVE-1903.  (The table location contains no files, and the string
                // representation of its path does not have a trailing slash.)
                match =
                        splitPath.equals(key) || splitPathWithNoSchema.equals(key);
            } else {
                // But for native tables, we need to do a prefix match for
                // subdirectories.  (Unlike non-native tables, prefix mixups don't seem
                // to be a potential problem here since we are always dealing with the
                // path to something deeper than the table location.)
                match =
                        splitPath.startsWith(key) || splitPathWithNoSchema.startsWith(key);
            }
            if (match) {
                final ArrayList<String> list = entry.getValue();
                for (final String val : list) {
                    aliases.add(val);
                }
            }
        }
        LOG.error("aliases szie " + aliases.size());
        for (final String alias : aliases) {
            final Operator<? extends Serializable> op = this.mrwork.getAliasToWork().get(
                    alias);
            if (op != null && op instanceof TableScanOperator) {
                final TableScanOperator tableScan = (TableScanOperator) op;

                // push down projections
                final ArrayList<Integer> list = tableScan.getNeededColumnIDs();

                if (list != null) {
                    LOG.error("tableScan.getNeededColumnIDs() :  " + list.toString() );
                    ColumnProjectionUtils.appendReadColumnIDs(jobConf, list);
                } else {
                    LOG.error("olumnProjectionUtils.setFullyReadColumns(jobConf);:  ");
                    ColumnProjectionUtils.setFullyReadColumns(jobConf);
                }

                pushFilters(jobConf, tableScan);
            }
        }
    }

    private void pushFilters(final JobConf jobConf, final TableScanOperator tableScan) {

        final TableScanDesc scanDesc = tableScan.getConf();
        LOG.error("scanDesc " + scanDesc );
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Filter text = " + filterText);
            LOG.debug("Filter expression = " + filterExprSerialized);
        }
        jobConf.set(
                TableScanDesc.FILTER_TEXT_CONF_STR,
                filterText);
        jobConf.set(
                TableScanDesc.FILTER_EXPR_CONF_STR,
                filterExprSerialized);
    }

    public JobConf cloneJobAndInit(final JobConf jobConf) {
        init(jobConf);
        final JobConf cloneJobConf = new JobConf(jobConf);

        // TODO FIX ME
        final Path tmpPath = new Path("/user/r.pecqueur/parquet");//((FileSplit) split).getPath().getParent().makeQualified(FileSystem.get(cloneJobConf)).toUri().getPath());

        final PartitionDesc part = getPartition(tmpPath.toString());
        LOG.error("bob part = " + part);
        Boolean nonNative = false;

        if ((part != null) && (part.getTableDesc() != null)) {
            LOG.error("bob part =  copyTableJobPropertiesToConf");
            Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), cloneJobConf);
            nonNative = part.getTableDesc().isNonNative();
        }

        pushProjectionsAndFilters(cloneJobConf, tmpPath.toString(), tmpPath.toUri().toString(), nonNative);

        return jobConf;
    }
}
