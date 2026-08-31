package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdCompressingInputStreamTest {
  @Test
  public void testSkip() throws IOException {
    String blobToSkip = "AAAAA"; // 5 bytes
    String blobToRead = "BBBBBBBBBBBBBBB"; // 15 bytes
    String blob = blobToSkip + blobToRead; // 20 bytes
    InputStream inputStream = new ByteArrayInputStream(blob.getBytes());
    ZstdCompressingInputStream zstdIn = new ZstdCompressingInputStream(inputStream);
    assertThat(zstdIn.skip(blobToSkip.length())).isEqualTo(blobToSkip.length());

    byte[] buf = new byte[20]; // compressed data can be larger than original data
    zstdIn.read(buf);
    String readBlob = new String(Zstd.decompress(buf, blobToRead.length()), StandardCharsets.UTF_8);
    assertThat(readBlob).isEqualTo(blobToRead);
  }

  // The constructor takes ownership of the source in its super() call, and hands the stream it
  // was building to nobody when it throws, so nothing else is left to close the source.
  @Test
  public void constructorClosesTheSourceWhenItThrows() {
    AtomicBoolean closed = new AtomicBoolean(false);
    InputStream source =
        new ByteArrayInputStream(new byte[0]) {
          @Override
          public void close() {
            closed.set(true);
          }
        };

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ZstdCompressingInputStream(source, ZstdCompressingInputStream.MIN_BUFFER_SIZE - 1));
    assertThat(closed.get()).isTrue();
  }
}
