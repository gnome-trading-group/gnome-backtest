package group.gnometrading.backtest.driver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** S3 held in memory: objects written with putObject come back from getObject, and any other key is missing. */
final class InMemoryS3 implements S3Client {

    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

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
