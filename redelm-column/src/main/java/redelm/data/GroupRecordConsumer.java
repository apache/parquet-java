package redelm.data;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import redelm.Log;
import redelm.io.RecordConsumer;

public class GroupRecordConsumer extends RecordConsumer {

  private final Deque<Group> groups = new ArrayDeque<Group>();
  private final Deque<Integer> fields = new ArrayDeque<Integer>();
  private final GroupFactory groupFactory;
  private final Collection<Group> result;

  public GroupRecordConsumer(GroupFactory groupFactory, Collection<Group> result) {
    this.groupFactory = groupFactory;
    this.result = result;
  }

  @Override
  public void startMessage() {
    groups.push(groupFactory.newGroup());
  }

  @Override
  public void endMessage() {
    if (Log.DEBUG) if (groups.size() != 1) throw new IllegalStateException("end of message in the middle of a record "+fields);
    this.result.add(groups.pop());
  }

  @Override
  public void startField(String field, int index) {
    fields.push(index);
  }

  @Override
  public void endField(String field, int index) {
    if (Log.DEBUG) if (!fields.peek().equals(index)) throw new IllegalStateException("opening "+fields.peek()+" but closing "+index+" ("+field+")");
    fields.pop();
  }

  @Override
  public void startGroup() {
    groups.push(groups.peek().addGroup(fields.peek()));
  }

  @Override
  public void endGroup() {
    groups.pop();
  }

  @Override
  public void addInt(int value) {
    groups.peek().add(fields.peek(), value);
  }

  @Override
  public void addString(String value) {
    groups.peek().add(fields.peek(), value);
  }

  @Override
  public void addBoolean(boolean value) {
    groups.peek().add(fields.peek(), value);
  }

  @Override
  public void addBinary(byte[] value) {
    groups.peek().add(fields.peek(), value);
  }

}