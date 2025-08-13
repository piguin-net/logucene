package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressInputStream extends FileInputStream {
    private final InputStream raw;
    private final File temp;

    public GzipCompressInputStream(InputStream raw, File temp) throws IOException {
        super(temp);
        this.raw = raw;
        this.temp = temp;
        this.temp.deleteOnExit();
        try (GZIPOutputStream output = new GZIPOutputStream(new FileOutputStream(this.temp));) {
            while (this.raw.available() > 0) {
                byte[] buf = this.raw.readNBytes(1024 * 1024);
                output.write(buf);
                output.flush();
            }
            output.finish();
        }
    }

    public GzipCompressInputStream(InputStream raw) throws IOException {
        this(raw, File.createTempFile("GzipCompressInputStream", ".gz"));
    }

    public void close() throws IOException {
        super.close();
        this.raw.close();
        this.temp.delete();
    }
}
