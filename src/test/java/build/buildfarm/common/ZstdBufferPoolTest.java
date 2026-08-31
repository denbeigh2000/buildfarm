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

import com.github.luben.zstd.Zstd;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdBufferPoolTest {
  private static final byte[] CONTENT = "Hello, World".getBytes(UTF_8);

  /** Fail a borrow that finds no free buffer, rather than wait for one. */
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
