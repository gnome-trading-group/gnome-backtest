package group.gnometrading.backtest.driver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * S3 held in memory: objects written with putObject come back from getObject and in listings, in one page, and any
 * other key is missing. Counts the gets asked for, found or not.
 */
final class InMemoryS3 implements S3Client {

    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final AtomicInteger gets = new AtomicInteger();

    int gets() {
        return gets.get();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
        try (InputStream stream = body.contentStreamProvider().newStream()) {
            objects.put(request.key(), stream.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return PutObjectResponse.builder().build();
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
        gets.incrementAndGet();
        byte[] bytes = objects.get(request.key());
        if (bytes == null) {
            throw NoSuchKeyException.builder()
                    .message("No object " + request.key())
                    .build();
        }
        return new ResponseInputStream<>(
                GetObjectResponse.builder().build(), AbortableInputStream.create(new ByteArrayInputStream(bytes)));
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        String prefix = request.prefix() == null ? "" : request.prefix();
        List<S3Object> contents = objects.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .sorted()
                .map(key -> S3Object.builder().key(key).build())
                .toList();
        return ListObjectsV2Response.builder()
                .contents(contents)
                .keyCount(contents.size())
                .isTruncated(false)
                .build();
    }

    @Override
    public <T> T getObject(GetObjectRequest request, ResponseTransformer<GetObjectResponse, T> transformer) {
        throw new UnsupportedOperationException("Only getObject(request) is used");
    }

    @Override
    public String serviceName() {
        return "s3";
    }

    @Override
    public void close() {}
}
