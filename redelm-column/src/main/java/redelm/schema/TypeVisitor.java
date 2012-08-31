package redelm.schema;

public interface TypeVisitor {
  void visit(GroupType groupType);
  void visit(MessageType messageType);
  void visit(PrimitiveType primitiveType);
}
