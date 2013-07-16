package parquet.column.page;

import parquet.bytes.BytesInput;

public class StatsPage extends Page {

  StatsPage(BytesInput bytes, int compressedSize, int uncompressedSize) {
    super(bytes, compressedSize, uncompressedSize);
  }

  @Override
  public void accept(PageVisitor visitor) {
    visitor.visit(this);
  }

}
