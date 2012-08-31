package redelm.io;

import java.util.Iterator;

import redelm.data.Group;

public class RecordWriter {

  private final MessageColumnIO messageColumnIO;

  public RecordWriter(MessageColumnIO messageColumnIO) {
    this.messageColumnIO = messageColumnIO;
  }

  public void write(Iterator<Group> messages) {
    while (messages.hasNext()) {
      Group group = (Group) messages.next();
      messageColumnIO.writeGroup(group, 0, 0);
    }
  }

  public void write(Group message) {
    messageColumnIO.writeGroup(message, 0, 0);
  }

}
