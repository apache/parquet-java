package parquet.filter2.compat;

import java.util.ArrayList;
import java.util.List;

import parquet.filter2.compat.FilterCompat.Filter;
import parquet.filter2.compat.FilterCompat.NoOpFilter;
import parquet.filter2.compat.FilterCompat.Visitor;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.SchemaCompatibilityValidator;
import parquet.filter2.statisticslevel.StatisticsFilter;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.schema.MessageType;

import static parquet.Preconditions.checkNotNull;

public class RowGroupFilter implements Visitor<List<BlockMetaData>> {
  private final List<BlockMetaData> blocks;
  private final MessageType schema;

  public static List<BlockMetaData> filterRowGroups(Filter filter, List<BlockMetaData> blocks, MessageType schema) {
    checkNotNull(filter, "filter");
    return filter.accept(new RowGroupFilter(blocks, schema));
  }

  private RowGroupFilter(List<BlockMetaData> blocks, MessageType schema) {
    this.blocks = checkNotNull(blocks, "blocks");
    this.schema = checkNotNull(schema, "schema");
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
    FilterPredicate filterPredicate = filterPredicateCompat.getFilterPredicate();

    // check that the schema of the filter matches the schema of the file
    SchemaCompatibilityValidator.validate(filterPredicate, schema);

    List<BlockMetaData> filteredBlocks = new ArrayList<BlockMetaData>();

    for (BlockMetaData block : blocks) {
      if (!StatisticsFilter.canDrop(filterPredicate, block.getColumns())) {
        filteredBlocks.add(block);
      }
    }

    return filteredBlocks;
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
    return blocks;
  }

  @Override
  public List<BlockMetaData> visit(NoOpFilter noOpFilter) {
    return blocks;
  }
}
