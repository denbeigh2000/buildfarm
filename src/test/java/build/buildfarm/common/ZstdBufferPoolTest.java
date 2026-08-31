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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.Zstd;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdBufferPoolTest {
  private static final byte[] CONTENT = "Hello, World".getBytes(UTF_8);

  /** A pool whose one buffer is out, and which fails a borrow rather than wait for one. */
  private static ZstdBufferPool exhaustedPool() throws Exception {
    ZstdBufferPool pool = new ZstdBufferPool(/* capacity= */ 1);
    pool.setMaxWait(Duration.ZERO);
    pool.borrowObject(); // the only buffer
    return pool;
  }

  // The stream borrows its buffer in its constructor, and the pool returns that constructor's
  // throw to the caller without the stream. Nobody but the pool can close the wrapped stream.
  @Test
  public void decompressingOutputStreamClosesTheDelegateOnAnExhaustedPool() throws Exception {
    try (ZstdBufferPool pool = exhaustedPool()) {
      AtomicBoolean closed = new AtomicBoolean(false);
      OutputStream delegate =
          new ByteArrayOutputStream() {
            @Override
            public void close() {
              closed.set(true);
            }
          };

      assertThrows(NoSuchElementException.class, () -> pool.newDecompressingOutputStream(delegate));
      assertThat(closed.get()).isTrue();
    }
  }

  @Test
  public void decompressingInputStreamClosesTheDelegateOnAnExhaustedPool() throws Exception {
    try (ZstdBufferPool pool = exhaustedPool()) {
      AtomicBoolean closed = new AtomicBoolean(false);
      InputStream delegate =
          new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() {
              closed.set(true);
            }
          };

      assertThrows(NoSuchElementException.class, () -> pool.newDecompressingInputStream(delegate));
      assertThat(closed.get()).isTrue();
    }
  }

  /** A pool that hands out real buffers and refuses to take them back. */
  private static final class RejectingPool implements BufferPool {
    private final ZstdBufferPool delegate;

    RejectingPool(ZstdBufferPool delegate) {
      this.delegate = delegate;
    }

    @Override
    public ByteBuffer get(int size) {
      return delegate.get(size);
    }

    @Override
    public void release(ByteBuffer buffer) {
      throw new IllegalStateException("pool is closed");
    }
  }

  // zstd returns the buffer to the pool before it closes anything, so a pool that refuses the
  // return leaves the wrapped stream open. Nothing above these streams closes it: the CAS write
  // waits on the closedFuture that the wrapped stream resolves.
  @Test
  public void decompressingOutputStreamClosesTheDelegateWhenTheBufferReturnFails()
      throws Exception {
    try (ZstdBufferPool pool = new ZstdBufferPool(/* capacity= */ 1)) {
      AtomicBoolean closed = new AtomicBoolean(false);
      OutputStream delegate =
          new ByteArrayOutputStream() {
            @Override
            public void close() {
              closed.set(true);
            }
          };
      ZstdDecompressingOutputStream decompressing =
          new ZstdDecompressingOutputStream(delegate, new RejectingPool(pool));

      assertThrows(IllegalStateException.class, decompressing::close);
      assertThat(closed.get()).isTrue();
    }
  }

  @Test
  public void decompressingInputStreamClosesTheDelegateWhenTheBufferReturnFails() throws Exception {
    try (ZstdBufferPool pool = new ZstdBufferPool(/* capacity= */ 1)) {
      AtomicBoolean closed = new AtomicBoolean(false);
      InputStream delegate =
          new ByteArrayInputStream(Zstd.compress(CONTENT)) {
            @Override
            public void close() {
              closed.set(true);
            }
          };
      ZstdDecompressingInputStream decompressing =
          new ZstdDecompressingInputStream(delegate, new RejectingPool(pool));

      assertThrows(IllegalStateException.class, decompressing::close);
      assertThat(closed.get()).isTrue();
    }
  }

  @Test
  public void decompressingOutputStreamReturnsItsBufferOnClose() throws Exception {
    try (ZstdBufferPool pool = new ZstdBufferPool(/* capacity= */ 1)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (OutputStream decompressing = pool.newDecompressingOutputStream(out)) {
        assertThat(pool.getNumActive()).isEqualTo(1);
        decompressing.write(Zstd.compress(CONTENT));
      }
      assertThat(out.toByteArray()).isEqualTo(CONTENT);
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }

  @Test
  public void decompressingInputStreamReturnsItsBufferOnClose() throws Exception {
    try (ZstdBufferPool pool = new ZstdBufferPool(/* capacity= */ 1)) {
      byte[] content;
      try (InputStream in =
          pool.newDecompressingInputStream(new ByteArrayInputStream(Zstd.compress(CONTENT)))) {
        assertThat(pool.getNumActive()).isEqualTo(1);
        content = ByteStreams.toByteArray(in);
      }
      assertThat(content).isEqualTo(CONTENT);
      assertThat(pool.getNumActive()).isEqualTo(0);
    }
  }
}
