/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.tools;

import java.io.IOException;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.parquet.tools.command.Command;
import org.apache.parquet.tools.command.Registry;

public class Main {
  public static final Options OPTIONS;
  public static PrintStream out;
  public static PrintStream err;

  static {
    OPTIONS = new Options();
    Option help = OptionBuilder.withLongOpt("help")
                               .withDescription("Show this help string")
                               .create('h');

    Option ncol = OptionBuilder.withLongOpt("no-color")
                               .withDescription("Disable color output even if supported")
                               .create();

    Option debg = OptionBuilder.withLongOpt("debug")
                               .withDescription("Enable debug output")
                               .create();

    OPTIONS.addOption(help);
    OPTIONS.addOption(ncol);
    OPTIONS.addOption(debg);
  }

  private static final int WIDTH = 80;
  private static final int LEFT_PAD = 4;
  private static final int DESC_PAD = 2;

  public static void mergeOptionsInto(Options opt, Options opts) {
    if (opts == null) {
      return;
    }

    Collection<Option> all = opts.getOptions();
    if (all != null && !all.isEmpty()) {
      for (Option o : all) {
        opt.addOption(o);
      }
    }
  }

  public static Options mergeOptions(Options opt, Options... opts) {
    Options results = new Options();
    mergeOptionsInto(results, opt);
    for (Options o : opts) {
      mergeOptionsInto(results, o);
    }

    return results;
  }

  public static void showUsage(HelpFormatter format, PrintWriter err, String name, Command command) {
    Options options = mergeOptions(OPTIONS, command.getOptions());
    String[] usage = command.getUsageDescription();

    String ustr = name + " [option...]";
    if (usage != null && usage.length >= 1) {
      ustr = ustr + " " + usage[0];
    }

    format.printWrapped(err, WIDTH, name + ":\n" + command.getCommandDescription());
    format.printUsage(err, WIDTH, ustr);
    format.printWrapped(err, WIDTH, LEFT_PAD, "where option is one of:");
    format.printOptions(err, WIDTH, options, LEFT_PAD, DESC_PAD);

    if (usage != null && usage.length >= 2) {
      for (int i = 1; i < usage.length; ++i) {
        format.printWrapped(err, WIDTH, LEFT_PAD, usage[i]);
      }
    }
  }

  public static void showUsage(String name, Command command) {
    HelpFormatter format = new HelpFormatter();
    PrintWriter err = new PrintWriter(Main.err,true);

    Options options = command.getOptions();
    showUsage(format, err, "parquet-" + name, command);
  }

  public static void showUsage() {
    HelpFormatter format = new HelpFormatter();
    PrintWriter err = new PrintWriter(Main.err,true);
    Map<String,Command> all = Registry.allCommands();

    boolean first = true;
    for (Map.Entry<String,Command> entry : all.entrySet()) {
      String name = entry.getKey();
      Command command = entry.getValue();

      if (!first) err.println();
      first = false;

      showUsage(format, err, "parquet-tools " + name, command);
    }
  }

  public static void die(String message, boolean usage) {
    die(message, usage, null, null);
  }

  public static void die(Throwable th, boolean usage) {
    die(th, usage, null, null);
  }

  public static void die(String message, boolean usage, String name, Command command) {
    if (message != null) {
      Main.err.println(message);
      Main.err.println();
    }

    if (usage) {
      if (name == null && command == null) {
        showUsage();
      }
      else {
        showUsage(name, command);
      }
    }

    System.exit(1);
  }

  public static void die(Throwable th, boolean usage, String name, Command command) {
    die(th.toString(), usage, name, command);
  }

  public static void main(String[] args) {
    Main.out = System.out;
    Main.err = System.err;

    PrintStream VoidStream = new PrintStream(new OutputStream() {
      @Override
      public void write(int b) throws IOException {}
      @Override
      public void write(byte[] b) throws IOException {}
      @Override
      public void write(byte[] b, int off, int len) throws IOException {}
      @Override
      public void flush() throws IOException {}
      @Override
      public void close() throws IOException {}
    });
    System.setOut(VoidStream);
    System.setErr(VoidStream);

    if (args.length == 0) {
      die("No command specified", true, null, null);
    }

    String name = args[0];
    if ("-h".equals(name) || "--help".equals(name)) {
      showUsage();
      System.exit(0);
    }

    Command command = Registry.getCommandByName(name);
    if (command == null) {
      die("Unknown command: " + name, true, null, null);
    }

    boolean debug = false;
    try {
      String[] cargs = Arrays.copyOfRange(args, 1, args.length);

      Options opts = mergeOptions(OPTIONS, command.getOptions());
      boolean extra = command.supportsExtraArgs();

      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(opts == null ? new Options() : opts, cargs, extra);
      if (cmd.hasOption('h')) {
        showUsage(name, command);
        System.exit(0);
      }

      if (cmd.hasOption("no-color")) {
        System.setProperty("DISABLE_COLORS", "true");
      }

      debug = cmd.hasOption("debug");

      command.execute(cmd);
    } catch (ParseException ex) {
      if (debug) ex.printStackTrace(Main.err);
      die("Invalid arguments: " + ex.getMessage(), true, name, command);
    } catch (Throwable th) {
      if (debug) th.printStackTrace(Main.err);
      die(th, false, name, command);
    }
  }
}
