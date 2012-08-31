package redelm.schema;


public class MessageType extends GroupType {

  public MessageType(String name, Type... fields) {
    super(Repetition.REPEATED, name, fields);
    setFieldPath(new String[0]);
  }

  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return "message "+getName()+" {\n"
        + membersDisplayString("  ")
        +"}\n";
  }
}
