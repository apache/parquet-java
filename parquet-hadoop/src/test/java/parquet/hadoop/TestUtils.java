package parquet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestUtils {

  public static void enforceEmptyDir(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("can not delete path " + path);
      }
    }
    if (!fs.mkdirs(path)) {
      throw new IOException("can not create path " + path);
    }
  }
}
