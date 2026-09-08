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
package org.apache.parquet.column.values.symboltable.fsst;

import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_BASE;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_MASK;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_MAX;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.ICL_FREE;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.MAX_SYMBOLS;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.MAX_SYMBOL_LENGTH;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.column.values.symboltable.SymbolTableTrainer;
import org.apache.parquet.column.values.symboltable.SymbolTableType;
import org.apache.parquet.column.values.symboltable.TrainedSymbolTable;
import org.apache.parquet.column.values.symboltable.ValueBuffer;

/**
 * Chooses an FSST symbol table for a set of values.
 *
 * <p>Java port of the training half of the FSST reference implementation by Peter Boncz, Viktor Leis
 * and Thomas Neumann (CWI / TU Munich), MIT licensed, at https://github.com/cwida/fsst, commit
 * 89f49c580c6388acf3b6ed2a49e1bfde6c05e616. The algorithm is reproduced rather than reinterpreted:
 * a writer that mines symbols differently still produces readable files, but it produces different
 * tables, and then the compression ratio can no longer be compared against another implementation
 * to tell a correct port from a subtly broken one.
 *
 * <p>Training runs five rounds over a sample of the data. Each round compresses the sample with the
 * table built so far, counting how often each symbol occurs and how often each ordered pair of
 * symbols occurs back to back; frequent pairs are then concatenated into candidate symbols, scored
 * by how many bytes they would save, and the best 255 become the next round's table. Rounds use a
 * growing fraction of the sample, and the table kept at the end is the round that scored best rather
 * than the last one.
 *
 * <p>One departure is deliberate and is documented at {@link #CANDIDATE_ORDER}.
 */
public class FsstTrainer implements SymbolTableTrainer {

  /** Bytes of sample the trainer aims for. */
  static final int SAMPLE_TARGET = 1 << 14;

  /** Worst-case sample size, which also bounds the score of a round. */
  static final int SAMPLE_MAX_SIZE = 2 * SAMPLE_TARGET;

  /** Length of one run of bytes taken from a value into the sample. */
  static final int SAMPLE_LINE = 512;

  /** Seed of the sampler's hash chain, from the reference implementation. */
  private static final long SAMPLE_SEED = 4637947;

  /**
   * Order candidates are offered to the table in: best score first, and among equal scores the
   * numerically smaller symbol, then the shorter one.
   *
   * <p>The reference implementation collects candidates in a hash set and drains them through a
   * heap, so when two candidates tie on both score and numeric value the winner depends on the hash
   * set's iteration order, which its own standard library does not fix. That last tie is possible,
   * because a shorter symbol whose trailing bytes are zero has the same numeric value as a longer
   * one. Breaking it on length makes this port's output a function of the input alone, which is what
   * lets a table be compared against another implementation's at all. Any table this produces is
   * readable by any reader; only the tie-breaking rule would have to be agreed to make two writers
   * agree byte for byte.
   */
  private static final Comparator<Candidate> CANDIDATE_ORDER = Comparator.comparingLong(
          (Candidate candidate) -> candidate.gain)
      .reversed()
      .thenComparing((left, right) -> Long.compareUnsigned(left.value, right.value))
      .thenComparingInt(candidate -> candidate.length);

  private final FsstCounters counters = new FsstCounters();
  private final byte[] bestCounters = new byte[FsstCounters.backupSize()];
  private final Map<Candidate, Candidate> candidates = new HashMap<>();
  private final ValueBuffer sample = new ValueBuffer(SAMPLE_MAX_SIZE, SAMPLE_MAX_SIZE / SAMPLE_LINE);

  /** Fraction of the sample, out of 128, that the current round compresses. */
  private int sampleFraction;

  @Override
  public SymbolTableType type() {
    return SymbolTableType.FSST_8;
  }

  @Override
  public TrainedSymbolTable train(ValueBuffer values) {
    FsstSymbolTable table = buildSymbolTable(makeSample(values));
    Fsst8SymbolTable fileTable = Fsst8SymbolTable.of(table);
    return new TrainedSymbolTable(fileTable, new Fsst8CodeStreamEncoder(table, fileTable));
  }

  /**
   * Draws a sample of at least {@link #SAMPLE_TARGET} bytes as runs taken from randomly chosen
   * values, or returns the values themselves when there are fewer bytes than that.
   *
   * <p>The choice of runs is driven by a chain of the same hash the symbol lookup uses, seeded by a
   * constant, so it is already reproducible and is ported as it stands. Replacing it with a
   * different sampler, however reasonable, would change the table for the same input.
   */
  private ValueBuffer makeSample(ValueBuffer values) {
    int valueCount = values.valueCount();
    if (valueCount == 0 || values.byteCount() < SAMPLE_TARGET) {
      return values;
    }
    sample.reset();
    long random = FsstCodes.hash(SAMPLE_SEED);
    int lineLimit = valueCount + SAMPLE_MAX_SIZE / SAMPLE_LINE;
    while (sample.byteCount() < SAMPLE_TARGET && sample.valueCount() < lineLimit) {
      // Choose a value, skipping forward over empty ones.
      random = FsstCodes.hash(random);
      int index = (int) Long.remainderUnsigned(random, valueCount);
      while (values.length(index) == 0) {
        if (++index == valueCount) {
          index = 0;
        }
      }
      // Choose one of its runs.
      int runCount = 1 + ((values.length(index) - 1) / SAMPLE_LINE);
      random = FsstCodes.hash(random);
      int runStart = SAMPLE_LINE * (int) Long.remainderUnsigned(random, runCount);
      int runLength = Math.min(values.length(index) - runStart, SAMPLE_LINE);
      sample.add(values.data(), values.offset(index) + runStart, runLength);
    }
    return sample;
  }

  private FsstSymbolTable buildSymbolTable(ValueBuffer sampleValues) {
    FsstSymbolTable table = new FsstSymbolTable();
    FsstSymbolTable best = new FsstSymbolTable();
    int bestGain = -SAMPLE_MAX_SIZE; // the score if every byte had to be escaped
    table.terminator = chooseTerminator(sampleValues);

    for (sampleFraction = 8; ; sampleFraction += 30) {
      counters.reset();
      int gain = compressCount(table, sampleValues);
      if (gain >= bestGain) {
        counters.backupSingleCounts(bestCounters);
        best.copyFrom(table);
        bestGain = gain;
      }
      if (sampleFraction >= 128) {
        break; // five rounds, at fractions 8, 38, 68, 98 and 128
      }
      makeTable(table);
    }

    // Rebuild the best round's table from the counts that round produced. The fraction is 128 here,
    // so this pass only re-ranks the symbols it already has and creates no new ones.
    counters.restoreSingleCounts(bestCounters);
    makeTable(best);
    best.finish();
    return best;
  }

  /**
   * Picks the least frequent byte as the terminator, preferring the lowest such byte.
   *
   * <p>The terminator is appended to each run the compressor works on, which is what lets the
   * compressor read eight bytes at a time without a bounds check: a symbol containing the
   * terminator is never in the table, so a match cannot run past the end of the value.
   */
  private static int chooseTerminator(ValueBuffer values) {
    int[] byteHistogram = new int[256];
    byte[] data = values.data();
    for (int i = 0; i < values.valueCount(); i++) {
      int end = values.offset(i) + values.length(i);
      for (int position = values.offset(i); position < end; position++) {
        byteHistogram[data[position] & 0xFF]++;
      }
    }
    int terminator = 256;
    int minimum = SAMPLE_MAX_SIZE;
    for (int i = 255; i >= 0; i--) {
      if (byteHistogram[i] > minimum) {
        continue;
      }
      terminator = i;
      minimum = byteHistogram[i];
    }
    return terminator;
  }

  /**
   * Compresses the sample with the table as it stands, counting symbols and symbol pairs, and
   * returns the number of bytes the table saves over escaping everything.
   */
  private int compressCount(FsstSymbolTable table, ValueBuffer values) {
    int gain = 0;
    for (int index = 0; index < values.valueCount(); index++) {
      int position = values.offset(index);
      int end = position + values.length(index);
      if (sampleFraction < 128 && randomFraction(index) > sampleFraction) {
        continue; // earlier rounds skip most of the sample, which roughly halves the work
      }
      if (position >= end) {
        continue;
      }
      byte[] data = values.data();
      int start = position;
      int code1 = table.findLongestSymbol(data, position, end);
      position += FsstCodes.length(table.symbolDescriptors[code1]);
      gain += FsstCodes.length(table.symbolDescriptors[code1]) - (1 + escapeCost(code1));
      while (true) {
        // Count the symbol as it stands, that is, the option of not extending it.
        counters.count1Increment(code1);
        // As an alternative, count just its first byte, unless that is the same thing.
        if (FsstCodes.length(table.symbolDescriptors[code1]) != 1) {
          counters.count1Increment(data[start] & 0xFF);
        }
        if (position == end) {
          break;
        }

        start = position;
        int code2;
        if (position < end - 7) {
          // Eight bytes are available, so the three lookups can be done without bounds checks.
          long word = FsstCodes.loadSymbolBytes(data, position, MAX_SYMBOL_LENGTH);
          int bucket = FsstCodes.hashBucket(word);
          long bucketDescriptor = table.hashDescriptors[bucket];
          code2 = table.shortCodes[FsstCodes.first2(word)] & CODE_MASK;
          word = FsstCodes.maskWord(word, bucketDescriptor);
          if (bucketDescriptor < ICL_FREE && table.hashValues[bucket] == word) {
            code2 = FsstCodes.code(bucketDescriptor);
            position += FsstCodes.length(bucketDescriptor);
          } else if (code2 >= CODE_BASE) {
            position += 2;
          } else {
            code2 = table.byteCodes[FsstCodes.first(word)] & CODE_MASK;
            position += 1;
          }
        } else {
          code2 = table.findLongestSymbol(data, position, end);
          position += FsstCodes.length(table.symbolDescriptors[code2]);
        }

        gain += (position - start) - (1 + escapeCost(code2));

        if (sampleFraction < 128) { // the last round does not need pair counts
          // Count the pair, that is, the option of concatenating the two symbols.
          counters.count2Increment(code1, code2);
          // As an alternative, count extending by just the next byte, unless that is the same thing.
          if ((position - start) > 1) {
            counters.count2Increment(code1, data[start] & 0xFF);
          }
        }
        code1 = code2;
      }
    }
    return gain;
  }

  /** A value between 1 and 128, fixed for a given value index and round. */
  private int randomFraction(int index) {
    return 1 + (int) (FsstCodes.hash((long) (index + 1) * sampleFraction) & 127);
  }

  /** Cost in bytes a code adds beyond the one byte it always costs: one more if it escapes. */
  private static int escapeCost(int code) {
    return code < CODE_BASE ? 1 : 0;
  }

  /**
   * Replaces the table with the best-scoring candidates from the counts just gathered.
   *
   * <p>Candidates are the symbols already in the table, the concatenation of each counted pair, and
   * each symbol extended by one byte. Single-byte symbols are scored eight times higher than their
   * frequency warrants, which the reference implementation notes both lowers the escape rate and
   * speeds up compression and decompression.
   */
  private void makeTable(FsstSymbolTable table) {
    candidates.clear();

    // Force the terminator into the table by making it look like the most frequent symbol.
    int terminatorPosition = table.symbolCount != 0 ? CODE_BASE : table.terminator;
    counters.count1Set(terminatorPosition, 65535);

    int positionLimit = CODE_BASE + table.symbolCount;
    for (int pos1 = 0; pos1 < positionLimit; pos1++) {
      int count1 = counters.count1Next(pos1);
      pos1 = counters.scanPosition(); // the scan skips empty counters
      if (count1 == 0) {
        continue;
      }
      long value1 = table.symbolValues[pos1];
      int length1 = FsstCodes.length(table.symbolDescriptors[pos1]);
      addOrIncrement(value1, length1, (length1 == 1 ? 8L : 1L) * count1);

      if (sampleFraction >= 128 // the last round does not create new symbols
          || length1 == MAX_SYMBOL_LENGTH // this symbol cannot be extended
          || FsstCodes.first(value1) == table.terminator) { // and none may contain the terminator
        continue;
      }
      for (int pos2 = 0; pos2 < positionLimit; pos2++) {
        int count2 = counters.count2Next(pos1, pos2);
        pos2 = counters.scanPosition();
        if (count2 == 0) {
          continue;
        }
        long value2 = table.symbolValues[pos2];
        if (FsstCodes.first(value2) != table.terminator) {
          int length2 = FsstCodes.length(table.symbolDescriptors[pos2]);
          addOrIncrement(concatenate(value1, length1, value2), concatenatedLength(length1, length2), count2);
        }
      }
    }

    List<Candidate> ranked = new ArrayList<>(candidates.values());
    ranked.sort(CANDIDATE_ORDER);
    table.clear();
    for (Candidate candidate : ranked) {
      if (table.symbolCount >= MAX_SYMBOLS) {
        break;
      }
      // A candidate whose hash bucket is taken is dropped rather than retried, as upstream does.
      table.add(candidate.value, FsstCodes.icl(CODE_MASK, candidate.length));
    }
  }

  /**
   * Records a candidate symbol, or adds to the score of one already recorded.
   *
   * <p>Rare candidates are dropped, on a threshold that grows with the round. Upstream notes this
   * improves the compression ratio as well as the speed of training.
   */
  private void addOrIncrement(long value, int length, long count) {
    if (count < (5L * sampleFraction) / 128) {
      return;
    }
    Candidate candidate = new Candidate(value, length);
    Candidate existing = candidates.get(candidate);
    if (existing != null) {
      existing.gain += count * length;
    } else {
      candidate.gain = count * length;
      candidates.put(candidate, candidate);
    }
  }

  /**
   * Joins two symbols into one, truncated to the maximum symbol length.
   *
   * <p>The shift is safe because a symbol already at the maximum length is never extended, so
   * {@code firstLength} is at most seven here. That matters more in Java than in C: a shift count of
   * 64 would be taken modulo 64 and quietly leave the value unshifted.
   */
  private static long concatenate(long first, int firstLength, long second) {
    return (second << (8 * firstLength)) | first;
  }

  private static int concatenatedLength(int firstLength, int secondLength) {
    return Math.min(firstLength + secondLength, MAX_SYMBOL_LENGTH);
  }

  /**
   * A symbol under consideration and the bytes it would save.
   *
   * <p>Identity is the symbol, not the score, so that repeated proposals of the same symbol
   * accumulate. Hashing on the value alone matches upstream and stays consistent with equality.
   */
  private static final class Candidate {
    private final long value;
    private final int length;
    private long gain;

    Candidate(long value, int length) {
      this.value = value;
      this.length = length;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Candidate)) {
        return false;
      }
      Candidate that = (Candidate) other;
      return value == that.value && length == that.length;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }
  }

  static {
    // Guards the assumption the position arithmetic in makeTable relies on.
    if (CODE_BASE + MAX_SYMBOLS >= CODE_MAX) {
      throw new AssertionError("symbol positions must stay below " + CODE_MAX);
    }
  }
}
