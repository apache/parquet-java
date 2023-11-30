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
package org.apache.parquet.glob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.glob.GlobNode.Atom;
import org.apache.parquet.glob.GlobNode.GlobNodeSequence;
import org.apache.parquet.glob.GlobNode.OneOf;

final class GlobParser {
  private GlobParser() {}

  /**
   * Parse a String into a {@link GlobNodeSequence}
   * <p>
   * See {@link org.apache.parquet.Strings#expandGlob(String)}
   */
  public static GlobNodeSequence parse(String pattern) {
    /*
     * The parse algorithm works as follows, assuming we are parsing:
     * "apache{one,pre{x,y}post,two}parquet{a,b}"
     *
     * 1) Begin scanning the string until we find the first {
     *
     * 2) Now that we've found the beginning of a glob group, scan forwards
     *    until the end of this glob group (by counting { and } we see until we find
     *    the closing } for the group we found in step 1).
     *
     * 3) Once the matching closing } is found we need to do two things. First, everything
     *    from the end of the last group up to start of this group is an Atom, so in the example
     *    above, once we've found that "{one,pre{x,y}post,two}" is the first group, we need to grab
     *    "apache" and treat it as an atom and add it to our sequence.
     *    Then, we parse "{one,pre{x,y}post,two}" using a similar but slightly different function (parseOneOf)
     *    and add the result from that to our sequence.
     *
     * 4) Repeat until the end of the string -- so next we find {a,b} and add "parquet" as an Atom and parse
     *    {a,b} using parseOneOf.
     */

    if (pattern.isEmpty() || pattern.equals("{}")) {
      return new GlobNodeSequence(Arrays.<GlobNode>asList(new Atom("")));
    }

    // the outer parse method needs to parse the pattern into a
    // GlobNodeSequence, though it may end up being a singleton sequence
    List<GlobNode> children = new ArrayList<GlobNode>();

    int unmatchedBraces = 0; // count of unmatched braces
    int firstBrace = 0; // open brace of current group being processsed
    int anchor = 0; // first un-parsed character position

    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);

      switch (c) {
        case ',':
          if (unmatchedBraces == 0) {
            // commas not allowed in the top level expression
            // TODO: maybe turn this check off?
            throw new GlobParseException(
                "Unexpected comma outside of a {} group:\n" + annotateMessage(pattern, i));
          }
          break;
        case '{':
          if (unmatchedBraces == 0) {
            // this is the first brace of an outermost {} group
            firstBrace = i;
          }
          unmatchedBraces++;
          break;
        case '}':
          unmatchedBraces--;
          if (unmatchedBraces < 0) {
            throw new GlobParseException("Unexpected closing }:\n" + annotateMessage(pattern, i));
          }
          if (unmatchedBraces == 0) {
            // grab everything from the end of the last group up to here,
            // not including the close brace, it is an Atom in our sequence
            // (assuming it's not empty)
            if (anchor != firstBrace) {
              // not empty!
              // (substring's end param is exclusive)
              children.add(new Atom(pattern.substring(anchor, firstBrace)));
            }

            // grab the group, parse it, add it to our sequence, and then continue
            // note that we skip the braces on both sides (substring's end param is exclusive)
            children.add(parseOneOf(pattern.substring(firstBrace + 1, i)));

            // we have now parsed all the way up to here, the next un-parsed char is i + 1
            anchor = i + 1;
          }
          break;
      }
    }

    if (unmatchedBraces > 0) {
      throw new GlobParseException("Not enough close braces in: " + pattern);
    }

    if (anchor != pattern.length()) {
      // either there were no {} groups, or there were some characters after the
      // last }, either way whatever is left (could be the entire input) is an Atom
      // in our sequence
      children.add(new Atom(pattern.substring(anchor, pattern.length())));
    }

    return new GlobNodeSequence(children);
  }

  private static OneOf parseOneOf(String pattern) {
    /*
     * This method is only called when parsing the inside of a {} expression.
     * So in the example above, of calling parse("apache{one,pre{x,y}post,two}parquet{a,b}")
     * this method will get called on first "one,pre{x,y}post,two", then on "x,y" and then on "a,b"
     *
     * The inside of a {} expression essentially means "one of these comma separated expressions".
     * So this gets parsed slightly differently than the top level string passed to parse().
     *
     * The algorithm works as follows:
     * 1) Split the string on ',' -- but only commas that are not inside of {} expressions
     * 2) Each of the splits can be parsed via the parse() method above
     * 3) Add all parsed splits to a single parent OneOf.
     */

    // this inner parse method needs to parse the pattern into a
    // OneOf, though it may end up being a singleton OneOf
    List<GlobNode> children = new ArrayList<GlobNode>();

    int unmatchedBraces = 0; // count of unmatched braces
    int anchor = 0; // first un-parsed character position

    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);

      switch (c) {
        case ',':
          // only "split" on commas not nested inside of {}
          if (unmatchedBraces == 0) {
            // ok, this comma is not inside of a {}, so
            // grab everything from anchor to here, parse it, and add it
            // as one of the options in this OneOf
            children.add(parse(pattern.substring(anchor, i)));

            // we have now parsed up to this comma, the next un-parsed char is i + 1
            anchor = i + 1;
          }
          break;
        case '{':
          unmatchedBraces++;
          break;
        case '}':
          unmatchedBraces--;
          if (unmatchedBraces < 0) {
            throw new GlobParseException("Unexpected closing }:\n" + annotateMessage(pattern, i));
          }
          break;
      }
    }

    if (unmatchedBraces > 0) {
      throw new GlobParseException("Not enough close braces in: " + pattern);
    }

    if (anchor != pattern.length()) {
      // either there were no commas outside of {} groups, or there were some characters after the
      // last comma, either way whatever is left (could be the entire input) is an Atom
      // in our sequence
      children.add(parse(pattern.substring(anchor, pattern.length())));
    }

    if (pattern.length() > 0 && pattern.charAt(pattern.length() - 1) == ',') {
      // the above loop won't handle a trailing comma
      children.add(parse(""));
    }

    return new OneOf(children);
  }

  // for pretty printing which character had the error
  private static String annotateMessage(String message, int pos) {
    StringBuilder sb = new StringBuilder(message);
    sb.append('\n');
    for (int i = 0; i < pos; i++) {
      sb.append('-');
    }
    sb.append('^');
    return sb.toString();
  }

  public static class GlobParseException extends RuntimeException {
    public GlobParseException() {}

    public GlobParseException(String message) {
      super(message);
    }

    public GlobParseException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
