package com.huawei.cloud.transfer.obs;


import com.obs.services.ObsClient;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.failure;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;

/**
 * The {@code ObsDataSource} class is an implementation of the {@code DataSource} interface
 * that provides a way to access data stored in the OBS (Huawei Object Storage Service).
 * <p>
 * This class supports opening a stream of data parts by listing OBS objects with a given prefix
 * and creating {@code ObsPart} instances for each object. It also provides a way to close the
 * connection to the OBS.
 * <p>
 * Example usage:
 * ObsDataSource source = ObsDataSource.Builder.newInstance()
 * .client(obsClient)
 * .bucketName(bucketName)
 * .keyPrefix("prefix")
 * .build();
 * StreamResult<Stream<Part>> streamResult = source.openPartStream();
 * <p>
 * Note: The usage example assumes that there is a configured ObsClient, a valid bucketName,
 * and a optional prefix for the OBS objects.
 */
public class ObsDataSource implements DataSource {
    private ObsClient client;
    private String bucketName;
    private String keyPrefix;

    @Override
    public StreamResult<Stream<Part>> openPartStream() {

        var obsObjects = listObjectsWithPrefix(bucketName, keyPrefix);

        if (obsObjects.isEmpty()) {
            return failure(new StreamFailure(List.of("Error listing OBS Objects: Object not found"), StreamFailure.Reason.NOT_FOUND));
        }

        var parts = obsObjects.stream()
                .map(ObsObject::getObjectKey)
                .map(key -> (Part) new ObsPart(client, key, bucketName));
        return success(parts);

    }

    private List<ObsObject> listObjectsWithPrefix(String bucketName, @Nullable String prefix) {
        List<ObsObject> objects = new ArrayList<>();
        var rq = new ListObjectsRequest(bucketName);
        if (prefix != null) {
            rq.setPrefix(prefix);
        }
        ObjectListing result;
        do {
            result = client.listObjects(rq);
            objects.addAll(result.getObjects());
            rq.setMarker(result.getNextMarker());
        } while (result.isTruncated());

        return objects;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    public record ObsPart(ObsClient client, String keyName, String bucketName) implements Part {

        @Override
        public String name() {
            return keyName;
        }

        @Override
        public long size() {
            return Part.super.size();
        }

        @Override
        public InputStream openStream() {
            var request = new GetObjectRequest(bucketName, keyName);
            return client.getObject(request).getObjectContent();
        }
    }

    public static class Builder {
        private final ObsDataSource source;

        private Builder() {
            source = new ObsDataSource();
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder bucketName(String bucketName) {
            source.bucketName = bucketName;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            source.keyPrefix = keyPrefix;
            return this;
        }

        public Builder client(ObsClient client) {
            source.client = client;
            return this;
        }

        public ObsDataSource build() {
            return source;
        }
    }
}
