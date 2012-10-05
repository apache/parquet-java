package redelm.pig;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrintFooter {
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage PrintFooter <path>");
      return;
    }
    Path path = new Path(new URI(args[0]));
//    System.out.println("Path: "+path);
    FileSystem fs = path.getFileSystem(new Configuration());
//    System.out.println("looking up in " + fs.getCanonicalServiceName());
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDir()) {
      System.err.println("provided path is a directory and not a file: " + path);
    }
    Footer footer = RedelmFileReader.readFooter(fs.open(path), fileStatus.getLen());
    System.out.println(Footer.toPrettyJSON(footer));
  }
}
