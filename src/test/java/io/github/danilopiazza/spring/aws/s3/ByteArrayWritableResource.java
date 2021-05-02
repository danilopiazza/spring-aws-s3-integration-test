package io.github.danilopiazza.spring.aws.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.WritableResource;

class ByteArrayWritableResource extends AbstractResource implements WritableResource {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public byte[] toByteArray() {
        return out.toByteArray();
    }

    @Override
    public String getDescription() {
        return "Byte array writable resource";
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new ByteArrayInputStream(out.toByteArray());
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return out;
    }
}
