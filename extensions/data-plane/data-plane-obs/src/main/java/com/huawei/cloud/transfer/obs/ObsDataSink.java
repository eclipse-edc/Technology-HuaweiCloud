package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.PartEtag;
import com.obs.services.model.UploadPartRequest;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

/**
 * This class represents a data sink for uploading data to an OBS (Object Storage Service) bucket.
 * It extends the ParallelSink class.
 */
public class ObsDataSink extends ParallelSink {

    private String bucketName;
    private int chunkSize;
    private ObsClient obsClient;

    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
        for (var part : parts) {
            var partNumber = 1;
            var bytesTransferred = 0L;
            try (var input = part.openStream()) {
                var completedParts = new ArrayList<PartEtag>();
                var request = new InitiateMultipartUploadRequest(bucketName, part.name());
                var uploadId = obsClient.initiateMultipartUpload(request).getUploadId();
                //todo: parallelize? It is supported by OBS: https://support.huaweicloud.com/eu/sdk-java-devg-obs/obs_21_0607.html#section3
                while (true) {
                    var bytesChunk = input.readNBytes(chunkSize);

                    if (bytesChunk.length < 1) {
                        break;
                    }
                    var uploadRequest = new UploadPartRequest(bucketName, part.name());
                    uploadRequest.setUploadId(uploadId);
                    uploadRequest.setPartNumber(partNumber);
                    uploadRequest.setPartSize((long) bytesChunk.length);
                    uploadRequest.setOffset(bytesTransferred);
                    uploadRequest.setInput(new ByteArrayInputStream(bytesChunk));

                    var uploadResult = obsClient.uploadPart(uploadRequest);

                    bytesTransferred += bytesChunk.length;
                    System.out.printf("transferred part %s, bytes: %s%n", partNumber, bytesTransferred);

                    completedParts.add(new PartEtag(uploadResult.getEtag(), uploadResult.getPartNumber()));
                    partNumber++;
                }
                var competeRequest = new CompleteMultipartUploadRequest(bucketName, part.name(), uploadId, completedParts);
                obsClient.completeMultipartUpload(competeRequest);
            } catch (Exception e) {
                return uploadFailure(e, part.name(), partNumber);
            }
        }

        return StreamResult.success();
    }

    @NotNull
    private StreamResult<Object> uploadFailure(Exception e, String keyName, int partNumber) {
        String msg = e instanceof ObsException ?
                ((ObsException) e).getErrorMessage() :
                e.getMessage();
        var message = format("Error writing part %s of the %s object on the %s bucket: %s.", partNumber, keyName, bucketName, msg);
        monitor.severe(message, e);
        return StreamResult.error(message);
    }

    public static class Builder extends ParallelSink.Builder<Builder, ObsDataSink> {

        private Builder() {
            super(new ObsDataSink());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder client(ObsClient client) {
            sink.obsClient = client;
            return this;
        }

        public Builder bucketName(String bucketName) {
            sink.bucketName = bucketName;
            return this;
        }

        public Builder chunkSizeBytes(int chunkSize) {
            sink.chunkSize = chunkSize;
            return this;
        }

        @Override
        protected void validate() {
            Objects.requireNonNull(sink.bucketName, "Must have a bucket name");
            Objects.requireNonNull(sink.obsClient, "Must have an obsClient");
        }
    }
}
