/**
 * Copyright 2013 ARRIS, Inc.
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
package parquet.tools.command;

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.tools.Main;

public class SizeCommand extends ArgsOnlyCommand {
  private FileStatus[] inputFileStatuses;
  private Configuration conf;
  private Path inputPath;
  private PrintWriter out;
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to get size & human readable size to stdout"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option help = OptionBuilder.withLongOpt("pretty")
                               .withDescription("Pretty size")
                               .create('p');
    OPTIONS.addOption(help);
    Option uncompressed = OptionBuilder.withLongOpt("uncompressed")
    							.withDescription("Uncompressed size")
    							.create('u');
    OPTIONS.addOption(uncompressed);
    Option detailed = OptionBuilder.withLongOpt("detailed")
    							   .withDescription("Detailed size of each matching file")
    							   .create('d');
    OPTIONS.addOption(detailed);
  }
  
  public SizeCommand() {
    super(1, 1);
  }

  @Override
  public Options getOptions() {
    return OPTIONS;
  }
  
  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public void execute(CommandLine options) throws Exception {
	super.execute(options);

    String[] args = options.getArgs();
    String input = args[0];
    out = new PrintWriter(Main.out, true);
    inputPath = new Path(input);
    conf = new Configuration();
    inputFileStatuses = inputPath.getFileSystem(conf).globStatus(inputPath);
    long size = 0;
    for(FileStatus fs : inputFileStatuses){
    	long fileSize = 0;
    	for(Footer f : ParquetFileReader.readFooters(conf, fs, false)){
    		for(BlockMetaData b : f.getParquetMetadata().getBlocks()){
    			size += (options.hasOption('u') ? b.getTotalByteSize() : b.getCompressedSize());
    			fileSize += (options.hasOption('u') ? b.getTotalByteSize() : b.getCompressedSize());
        	}
    	}
    	if(options.hasOption('d')){
    		if(options.hasOption('p')){ 
    			out.format("%s: %s\n", fs.getPath().getName(), getPrettySize(fileSize));
    		}
    		else{
    			out.format("%s: %d bytes\n", fs.getPath().getName(), fileSize);
    		}
    	}
    }
    
    if(options.hasOption('p')){
    	out.format("Total Size: %s", getPrettySize(size));
    }
    else{
    	out.format("Total Size: %d bytes", size);
    }
    out.println();
  }
  
  public String getPrettySize(long bytes){
		double oneKB = 1024;
		double oneMB = oneKB * 1024;
		double oneGB = oneMB * 1014;
		double oneTB = oneGB * 1024;
		double onePB = oneTB * 1024;
		if (bytes/oneKB < 1){
			return  String.format("%.3f", bytes) + " bytes";
		}
		if (bytes/oneMB < 1){
			return String.format("%.3f", bytes/oneKB) + " KB";			
		}
		if (bytes/oneGB < 1){
			return String.format("%.3f", bytes/oneMB) + " MB";			
		}
		if (bytes/oneTB < 1){
			return String.format("%.3f", bytes/oneGB) + " GB";
		}
		return String.valueOf(bytes/onePB) + " PB";		
	}
}
