// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import com.github.luben.zstd.Zstd;
import com.google.common.base.Stopwatch;
import io.prometheus.client.CollectorRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdDecompressingOutputStreamTest {
  private static final Duration BORROW_TIMEOUT = Duration.ofMillis(500);

  private static OutputStream sink() {
    return new ByteArrayOutputStream();
  }

  // A ZstdDecompressingOutputStream takes one buffer for its whole lifetime, so a pool of one lets
  // a single open stream exhaust it.
  private static FixedBufferPool singleBufferPool() {
    return new FixedBufferPool(/* capacity= */ 1, BORROW_TIMEOUT);
  }

  @Test
  public void decompressesWhatZstdCompressed() throws IOException {
    byte[] blob = "the quick brown fox jumps over the lazy dog".getBytes(UTF_8);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (FixedBufferPool pool = singleBufferPool();
        ZstdDecompressingOutputStream zstdOut = new ZstdDecompressingOutputStream(out, pool)) {
      zstdOut.write(Zstd.compress(blob));
    }

    assertThat(out.toByteArray()).isEqualTo(blob);
  }

  /**
   * Without a borrow timeout the second stream waits forever, and every later zstd transfer on the
   * process queues behind it. Nothing else in the stack bounds this wait.
   */
  @Test
  public void borrowFailsOnceTheTimeoutPasses() throws IOException {
    try (FixedBufferPool pool = singleBufferPool();
        ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool)) {
      Stopwatch stopwatch = Stopwatch.createStarted();

      // zstd-jni turns the null that the exhausted pool returns into a ZstdIOException.
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));

      assertThat(stopwatch.elapsed().toMillis()).isAtLeast(BORROW_TIMEOUT.toMillis());
      assertThat(pool.getNumActive()).isEqualTo(1);
    }
  }

  /** A failed borrow must not consume a buffer, or the pool loses one buffer per timeout. */
  @Test
  public void closeReturnsTheBufferAfterATimeout() throws IOException {
    try (FixedBufferPool pool = singleBufferPool()) {
      ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));
      held.close();

      assertThat(pool.getNumActive()).isEqualTo(0);
      new ZstdDecompressingOutputStream(sink(), pool).close();
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }

  /** The capacity-only constructor keeps the wait without a bound, which is the default. */
  @Test
  public void capacityOnlyPoolLeavesTheWaitUnbounded() throws IOException {
    try (FixedBufferPool pool = new FixedBufferPool(/* capacity= */ 1)) {
      assertThat(pool.getMaxWaitDuration().isNegative()).isTrue();

      new ZstdDecompressingOutputStream(sink(), pool).close();
      assertThat(pool.getNumActive()).isEqualTo(0);
      new ZstdDecompressingOutputStream(sink(), pool).close();
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }

  /**
   * The age and the thread of the oldest holder are what separate a stream that nobody closed from
   * ordinary contention. Neither the timeout itself nor the borrow counter tells them apart, so
   * this report is the only thing that does.
   */
  @Test
  public void exhaustedPoolReportsTheHoldersWithoutTheirSites() throws IOException {
    List<LogRecord> records = new ArrayList<>();
    try (FixedBufferPool pool = singleBufferPool();
        ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
        LogCapture capture = new LogCapture(records)) {
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));

      // One record, summary and holders together, so a log collector ships one event.
      assertThat(records).hasSize(1);
      assertThat(records.get(0).getMessage())
          .isEqualTo(
              "zstd buffer pool exhausted: 1/1 buffers active, 1 waiting"
                  + format("%n  held for 0s by %s", Thread.currentThread().getName()));
    }
  }

  /** Frames are the point of the borrow site switch, and they cost a stack trace per borrow. */
  @Test
  public void trackedPoolReportsTheBorrowSite() throws IOException {
    List<LogRecord> records = new ArrayList<>();
    try (FixedBufferPool pool =
            new FixedBufferPool(/* capacity= */ 1, BORROW_TIMEOUT, /* trackBorrowSites= */ true);
        ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
        LogCapture capture = new LogCapture(records)) {
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));

      assertThat(records).hasSize(1);
      assertThat(records.get(0).getMessage()).contains(FixedBufferPool.class.getName());
      assertThat(records.get(0).getMessage())
          .contains(ZstdDecompressingOutputStreamTest.class.getName());
    }
  }

  /**
   * invalidateObject destroys a buffer rather than returning it. A record left behind names a
   * holder that no longer exists, which is the one thing this report must not do.
   */
  @Test
  public void invalidatedBufferLeavesNoHolder() throws Exception {
    List<LogRecord> records = new ArrayList<>();
    try (FixedBufferPool pool = singleBufferPool()) {
      pool.invalidateObject(pool.borrowObject());

      try (ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
          LogCapture capture = new LogCapture(records)) {
        assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));
      }

      assertThat(records).hasSize(1);
      assertThat(records.get(0).getMessage().split("held for", -1)).hasLength(2);
    }
  }

  /**
   * An empty pool times out every waiter, and each report walks the holder map. Without the
   * throttle a saturated worker spends its time writing the same warning.
   */
  @Test
  public void repeatedTimeoutsReportOnce() throws IOException {
    List<LogRecord> records = new ArrayList<>();
    try (FixedBufferPool pool = singleBufferPool();
        ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
        LogCapture capture = new LogCapture(records)) {
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));
      int afterFirst = records.size();
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));

      assertThat(records).hasSize(afterFirst);
    }
  }

  /**
   * An interrupt and a timeout both hand zstd-jni a null, and it reports both with the same
   * message. The counter label is the only thing that separates a cancelled request from a pool
   * that cannot keep up.
   */
  @Test
  public void interruptedBorrowCountsApartFromATimeout() throws Exception {
    // Unbounded, so the borrow blocks until the interrupt rather than timing out.
    try (FixedBufferPool pool = new FixedBufferPool(/* capacity= */ 1)) {
      ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool);
      AtomicReference<Throwable> thrown = new AtomicReference<>();
      AtomicReference<Boolean> flagged = new AtomicReference<>();
      Thread borrower =
          new Thread(
              () -> {
                try {
                  new ZstdDecompressingOutputStream(sink(), pool).close();
                } catch (Throwable t) {
                  thrown.set(t);
                }
                flagged.set(Thread.currentThread().isInterrupted());
              });
      double interruptedBefore = borrowFailures("interrupted");
      double timeoutBefore = borrowFailures("timeout");

      borrower.start();
      while (pool.getNumWaiters() == 0) {
        Thread.onSpinWait();
      }
      borrower.interrupt();
      borrower.join();
      held.close();

      assertThat(thrown.get()).isInstanceOf(IOException.class);
      assertThat(flagged.get()).isTrue();
      assertThat(borrowFailures("interrupted") - interruptedBefore).isEqualTo(1.0);
      assertThat(borrowFailures("timeout") - timeoutBefore).isEqualTo(0.0);
    }
  }

  /** The wait distribution is what you alert on, so the metric has to carry every borrow. */
  @Test
  public void everyBorrowLandsOnTheWaitHistogram() throws IOException {
    double before = borrowWaitCount();
    try (FixedBufferPool pool = singleBufferPool();
        ZstdDecompressingOutputStream held = new ZstdDecompressingOutputStream(sink(), pool)) {
      assertThrows(IOException.class, () -> new ZstdDecompressingOutputStream(sink(), pool));
    }

    // The borrow that took the buffer, and the borrow that timed out waiting for it.
    assertThat(borrowWaitCount() - before).isEqualTo(2.0);
  }

  private static double borrowWaitCount() {
    Double value =
        CollectorRegistry.defaultRegistry.getSampleValue(
            "zstd_buffer_pool_borrow_wait_seconds_count");
    return value == null ? 0 : value;
  }

  // The registry is process wide and other tests in this JVM share it, so only deltas mean
  // anything here.
  private static double borrowFailures(String reason) {
    Double value =
        CollectorRegistry.defaultRegistry.getSampleValue(
            "zstd_buffer_pool_borrow_failures_total",
            new String[] {"reason"},
            new String[] {reason});
    return value == null ? 0 : value;
  }

  /** Collects what FixedBufferPool writes, which is the only view of its holder bookkeeping. */
  private static final class LogCapture extends Handler implements AutoCloseable {
    private static final Logger LOGGER =
        Logger.getLogger(ZstdDecompressingOutputStream.class.getName());

    private final List<LogRecord> records;

    LogCapture(List<LogRecord> records) {
      this.records = records;
      LOGGER.addHandler(this);
    }

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {
      LOGGER.removeHandler(this);
    }
  }
}
