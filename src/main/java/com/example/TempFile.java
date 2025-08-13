package com.example;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class TempFile extends File implements Closeable {

    public TempFile(String prefix, String suffix) throws IOException {
        this(File.createTempFile(prefix, suffix));
    }

    private TempFile(File file) {
        super(file.getAbsolutePath());
        file.deleteOnExit();
    }

    @Override
    public void close() {
        this.delete();
    }
    
}
