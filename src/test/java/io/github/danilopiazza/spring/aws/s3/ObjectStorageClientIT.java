package io.github.danilopiazza.spring.aws.s3;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
class ObjectStorageClientIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageClientIT.class);

    static DockerImageName localstackImage = DockerImageName.parse("localstack/localstack");

    @Container
    static LocalStackContainer localStack = new LocalStackContainer(localstackImage).withServices(S3);

    @Autowired
    ObjectStorageClient client;

    @Value("${aws.s3.bucket.name}")
    String bucketName;

    AmazonS3 s3;
    String key;
    byte[] contents;

    @BeforeAll
    static void initAll() {
        System.setProperty("cloud.aws.s3.endpoint", localStack.getEndpointOverride(S3).toString());
        System.setProperty("cloud.aws.credentials.access-key", localStack.getAccessKey());
        System.setProperty("cloud.aws.credentials.secret-key", localStack.getSecretKey());
        System.setProperty("cloud.aws.region.static", localStack.getRegion());
        LOGGER.info("LocalStack AWS S3 Endpoint: {}", localStack.getEndpointOverride(S3));
        LOGGER.info("LocalStack AWS Access Key: {}", localStack.getAccessKey());
        LOGGER.info("LocalStack AWS Secret Key: {}", localStack.getSecretKey());
        LOGGER.info("LocalStack AWS Region: {}", localStack.getRegion());
    }

    @BeforeEach
    void init() {
        s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStack.getEndpointConfiguration(S3))
                .withCredentials(localStack.getDefaultCredentialsProvider())
                .build();
        s3.createBucket(bucketName);

        key = UUID.randomUUID().toString();
        contents = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void testGetObject() throws Exception {
        s3.putObject(bucketName, key, new String(contents, StandardCharsets.UTF_8));

        assertArrayEquals(contents, client.getObject(key));
    }

    @Test
    void testGetObjectNoSuchKey() throws Exception {
        AmazonS3Exception exception = assertThrows(AmazonS3Exception.class, () -> client.getObject(key));

        assertEquals("NoSuchKey", exception.getErrorCode());
    }

    @Test
    void testPutObject() throws Exception {
        client.putObject(key, contents);

        try (InputStream in = s3.getObject(bucketName, key).getObjectContent()) {
            assertArrayEquals(contents, in.readAllBytes());
        }
    }
}
