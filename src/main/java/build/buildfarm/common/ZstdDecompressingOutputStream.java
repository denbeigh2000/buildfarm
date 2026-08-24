// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

// The decompressing stream came from the bazel project. The buffer pool below it, and the
// reporting around that pool, did not.
package build.buildfarm.common;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import build.buildfarm.common.io.FeedbackOutputStream;
import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/** An {@link OutputStream} that use zstd to decompress the content. */
@Log
public final class ZstdDecompressingOutputStream extends FeedbackOutputStream {
  private final OutputStream out;
  private ByteArrayInputStream inner;
  private final ZstdInputStreamNoFinalizer zis;

  private static final class ZstdDInBufferFactory extends BasePooledObjectFactory<ByteBuffer> {
    static int getBufferSize() {
      return (int) ZstdInputStreamNoFinalizer.recommendedDInSize();
    }

    @Override
    public ByteBuffer create() {
      return ByteBuffer.allocate(getBufferSize());
    }

    @Override
    public PooledObject<ByteBuffer> wrap(ByteBuffer buffer) {
      return new DefaultPooledObject<>(buffer);
    }
  }

  public static final class FixedBufferPool extends GenericObjectPool<ByteBuffer> {
    /** Records who took a buffer, so that an exhausted pool can name the holders. */
    private record Borrow(long startNanos, String thread) {}

    private static final Duration EXHAUSTED_LOG_INTERVAL = Duration.ofSeconds(30);
    private static final int EXHAUSTED_LOG_HOLDERS = 3;

    // ByteBuffer.equals compares contents, so two distinct idle buffers of the same size are equal
    // to each other. A HashMap here would let one borrow record overwrite another.
    private final Map<ByteBuffer, Borrow> borrows =
        Collections.synchronizedMap(new IdentityHashMap<>());
    private final AtomicLong lastExhaustedLogNanos =
        new AtomicLong(System.nanoTime() - EXHAUSTED_LOG_INTERVAL.toNanos());

    private static GenericObjectPoolConfig<ByteBuffer> createPoolConfig(
        int capacity, Duration maxWait) {
      GenericObjectPoolConfig<ByteBuffer> poolConfig = new GenericObjectPoolConfig<>();
      poolConfig.setMaxTotal(capacity);
      poolConfig.setMaxWait(maxWait);
      return poolConfig;
    }

    public FixedBufferPool(int capacity) {
      this(capacity, BaseObjectPoolConfig.DEFAULT_MAX_WAIT);
    }

    /**
     * @param maxWait how long a borrow waits for a free buffer. A negative duration waits without a
     *     bound, which can stall every zstd transfer on this process. A zero duration fails a
     *     borrow that cannot take a free buffer at once.
     */
    public FixedBufferPool(int capacity, Duration maxWait) {
      super(new ZstdDInBufferFactory(), createPoolConfig(capacity, maxWait));
    }

    // borrowObject() and borrowObject(long) both dispatch here, so this covers every entry
    // point. Confirmed with commons-pool2 2.13.1.
    @Override
    public ByteBuffer borrowObject(Duration maxWaitDuration) throws Exception {
      try {
        ByteBuffer buffer = super.borrowObject(maxWaitDuration);
        borrows.put(buffer, new Borrow(System.nanoTime(), Thread.currentThread().getName()));
        return buffer;
      } catch (NoSuchElementException e) {
        logExhausted();
        throw e;
      }
    }

    @Override
    public void returnObject(ByteBuffer buffer) {
      borrows.remove(buffer);
      super.returnObject(buffer);
    }

    // invalidateObject destroys a buffer instead of returning it, and invalidateObject(T)
    // dispatches here, so this is the other way a borrow ends. Without it the record outlives
    // the buffer and the report names a holder that is gone.
    @Override
    public void invalidateObject(ByteBuffer buffer, DestroyMode destroyMode) throws Exception {
      borrows.remove(buffer);
      super.invalidateObject(buffer, destroyMode);
    }

    /**
     * Report the pool state and the oldest holders. A holder that is minutes old is a stream that
     * nobody closed; holders that are all seconds old are ordinary contention. The two need
     * different fixes, and the borrow timeout alone does not tell them apart.
     */
    private void logExhausted() {
      // An empty pool fails every waiter, and each report walks the holder map.
      long now = System.nanoTime();
      long last = lastExhaustedLogNanos.get();
      if (now - last < EXHAUSTED_LOG_INTERVAL.toNanos()
          || !lastExhaustedLogNanos.compareAndSet(last, now)) {
        return;
      }

      List<Borrow> holders;
      synchronized (borrows) {
        // Copy the values, not the entries: IdentityHashMap hands the same Entry instance back on
        // every step of its iterator.
        holders = new ArrayList<>(borrows.values());
      }
      holders.sort(Comparator.comparingLong(Borrow::startNanos));

      // One record, so that a log collector ships the summary and its holders as one event.
      StringBuilder report =
          new StringBuilder(
              format(
                  // Whoever reports has already left the take queue, so count them back in.
                  "zstd buffer pool exhausted: %d/%d buffers active, %d waiting",
                  getNumActive(), getMaxTotal(), getNumWaiters() + 1));
      for (Borrow holder : holders.subList(0, min(EXHAUSTED_LOG_HOLDERS, holders.size()))) {
        report.append(
            format(
                "%n  held for %ds by %s",
                NANOSECONDS.toSeconds(now - holder.startNanos()), holder.thread()));
      }
      log.log(Level.WARNING, report.toString());
    }
  }

  public static class ZstdFixedBufferPool implements BufferPool {
    private final FixedBufferPool pool;

    public ZstdFixedBufferPool(FixedBufferPool pool) {
      this.pool = pool;
    }

    @Override
    public ByteBuffer get(int bufferSize) {
      // guaranteed through final
      checkState(bufferSize > 0 && bufferSize <= ZstdDInBufferFactory.getBufferSize());
      try {
        return pool.borrowObject();
      } catch (NoSuchElementException e) {
        // Do not throw. zstd-jni turns a null from a BufferPool into a ZstdIOException, so the
        // callers that already handle IOException see this as a failed transfer instead of an
        // unchecked exception out of a constructor. commons-pool2 raises NoSuchElementException
        // for a borrow timeout and for an exhausted pool alike, and both mean the same thing here.
        return null;
      } catch (InterruptedException e) {
        // Restore the flag. throwIfUnchecked would otherwise wrap this and lose it, and zstd-jni
        // reports it with the same message it uses for a timeout.
        Thread.currentThread().interrupt();
        return null;
      } catch (Exception e) {
        throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public void release(ByteBuffer buffer) {
      try {
        pool.returnObject(buffer);
      } catch (Exception e) {
        throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
  }

  public ZstdDecompressingOutputStream(OutputStream out, FixedBufferPool pool) throws IOException {
    this.out = out;
    zis =
        new ZstdInputStreamNoFinalizer(
                new InputStream() {
                  @Override
                  public int read() {
                    return inner.read();
                  }

                  @Override
                  public int read(byte[] b, int off, int len) {
                    return inner.read(b, off, len);
                  }
                },
                new ZstdFixedBufferPool(pool))
            .setContinuous(true);
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    inner = new ByteArrayInputStream(b, off, len);
    byte[] data = ByteString.readFrom(zis).toByteArray();
    out.write(data, 0, data.length);
  }

  @Override
  public void close() throws IOException {
    zis.close();
    out.close();
  }

  @Override
  public boolean isReady() {
    return true;
  }
}
