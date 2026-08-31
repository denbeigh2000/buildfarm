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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;

import build.buildfarm.common.io.FeedbackOutputStream;
import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * A fixed set of the working buffers that zstd decompression needs, and the only source of the
 * streams that borrow them.
 *
 * Why is this here? A zstd stream borrows its buffer inside its constructor, and returns the
 * buffer in close(). A constructor that throws has already wrapped the stream it was given, and it
 * hands the wrapper to nobody, so no caller can close either stream. Both decompressing streams
 * therefore have package private constructors, and every caller goes through the factory methods
 * below, which close the wrapped stream when the constructor throws.
 */
public final class ZstdBufferPool extends GenericObjectPool<ByteBuffer> implements BufferPool {
  private static final class ZstdDInBufferFactory extends BasePooledObjectFactory<ByteBuffer> {
    @Override
    public ByteBuffer create() {
      return ByteBuffer.allocate(bufferSize());
    }

    @Override
    public PooledObject<ByteBuffer> wrap(ByteBuffer buffer) {
      return new DefaultPooledObject<>(buffer);
    }
  }

  static int bufferSize() {
    return (int) ZstdInputStreamNoFinalizer.recommendedDInSize();
  }

  private static GenericObjectPoolConfig<ByteBuffer> createPoolConfig(int capacity) {
    GenericObjectPoolConfig<ByteBuffer> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(capacity);
    return poolConfig;
  }

  public ZstdBufferPool(int capacity) {
    super(new ZstdDInBufferFactory(), createPoolConfig(capacity));
  }

  @Override
  public ByteBuffer get(int size) {
    // Only the decompressing streams below borrow from this pool, and both ask zstd for
    // recommendedDInSize, which is what every buffer here holds.
    checkState(size > 0 && size <= bufferSize());
    try {
      return borrowObject();
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void release(ByteBuffer buffer) {
    try {
      returnObject(buffer);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /** Decompress everything written to the returned stream into {@code out}. */
  public FeedbackOutputStream newDecompressingOutputStream(OutputStream out) throws IOException {
    return takeOwnership(out, () -> new ZstdDecompressingOutputStream(out, this));
  }

  /** Decompress {@code in} as the returned stream is read. */
  public InputStream newDecompressingInputStream(InputStream in) throws IOException {
    return takeOwnership(in, () -> new ZstdInputStreamNoFinalizer(in, this));
  }

  private interface StreamConstructor<T> {
    T construct() throws IOException;
  }

  private static <T> T takeOwnership(Closeable stream, StreamConstructor<T> constructor)
      throws IOException {
    try {
      return constructor.construct();
    } catch (IOException | RuntimeException e) {
      try {
        stream.close();
      } catch (IOException closeError) {
        e.addSuppressed(closeError);
      }
      throw e;
    }
  }
}
