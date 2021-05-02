package io.github.danilopiazza.spring.aws.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ResourceLoader;

@ExtendWith(MockitoExtension.class)
class ObjectStorageClientTest {
    final String bucket = "test-bucket";
    final String key = "test-key";
    final byte[] contents = "test-contents".getBytes(StandardCharsets.UTF_8);
    ObjectStorageClient client;
    @Mock ResourceLoader resourceLoader;

    @BeforeEach
    void init() {
        client = new ObjectStorageClient(resourceLoader, bucket);
    }

    @Test
    void testGetObject() throws Exception {
        when(resourceLoader.getResource("s3://test-bucket/test-key")).thenReturn(new ByteArrayResource(contents));

        assertArrayEquals(contents, client.getObject(key));
    }

    @Test
    void testPutObject() throws Exception {
        ByteArrayWritableResource resource = new ByteArrayWritableResource();
        when(resourceLoader.getResource("s3://test-bucket/test-key")).thenReturn(resource);

        client.putObject(key, contents);

        assertArrayEquals(contents, resource.toByteArray());
    }
}
