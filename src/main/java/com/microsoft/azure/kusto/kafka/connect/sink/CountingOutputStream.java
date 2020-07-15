package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CountingOutputStream extends FilterOutputStream {
  public long numBytes = 0;
  public OutputStream outputStream;

  public CountingOutputStream(OutputStream out) {
    super(out);
    this.outputStream = out;
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
    this.numBytes++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
    this.numBytes += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
    this.numBytes += len;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }
}
