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

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} that uses zstd to decompress the content.
 *
 * <p>Build one with {@link ZstdBufferPool#newDecompressingInputStream}. The stream takes ownership
 * of {@code compressed} and closes it, and the pool closes {@code compressed} for the constructor
 * that throws.
 */
public final class ZstdDecompressingInputStream extends FilterInputStream {
  ZstdDecompressingInputStream(InputStream compressed, BufferPool pool) throws IOException {
    super(new ZstdInputStreamNoFinalizer(compressed, pool));
  }
}
