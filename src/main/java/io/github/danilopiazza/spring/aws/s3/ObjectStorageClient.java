package io.github.danilopiazza.spring.aws.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

@Component
public class ObjectStorageClient {
    private static final String S3_URI_SCHEME = "s3";

    private final ResourceLoader resourceLoader;
    private final String bucket;

    public ObjectStorageClient(@Autowired ResourceLoader resourceLoader, @Value("${aws.s3.bucket.name}") String bucket) {
        Assert.notNull(resourceLoader, "ResourceLoader must not be null");
        Assert.notNull(bucket, "Bucket must not be null");
        this.resourceLoader = resourceLoader;
        this.bucket = bucket;
    }

    public byte[] getObject(String key) throws IOException {
        Resource resource = resourceLoader.getResource(s3Uri(key).toUriString());
        try (InputStream in = resource.getInputStream()) {
            return StreamUtils.copyToByteArray(in);
        }
    }

    public void putObject(String key, byte[] contents) throws IOException {
        WritableResource resource = (WritableResource) resourceLoader.getResource(s3Uri(key).toUriString());
        try (OutputStream out = resource.getOutputStream()) {
            StreamUtils.copy(contents, out);
        }
    }

    private UriComponents s3Uri(String key) {
        return UriComponentsBuilder.newInstance().scheme(S3_URI_SCHEME).host(bucket).path(key).build();
    }
}
