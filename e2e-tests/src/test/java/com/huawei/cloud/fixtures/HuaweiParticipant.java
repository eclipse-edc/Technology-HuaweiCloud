package com.huawei.cloud.fixtures;

import com.fasterxml.jackson.annotation.JsonCreator;
import jakarta.json.JsonArray;
import org.eclipse.edc.test.system.utils.Participant;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.restassured.http.ContentType.JSON;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getFreePort;
import static org.eclipse.edc.spi.system.ServiceExtensionContext.PARTICIPANT_ID;

public class HuaweiParticipant extends Participant {

    private static final String IAM_OTC_CLOUD_URL = "https://iam.eu-de.otc.t-systems.com";

    private static final Duration TIMEOUT = Duration.ofMillis(10000);
    private Endpoint controlEndpoint;
    private String apiKey;

    public Endpoint getControlEndpoint() {
        return controlEndpoint;
    }

    public Map<String, String> controlPlaneConfiguration() {
        return new HashMap<>() {
            {
                put(PARTICIPANT_ID, id);
                put("edc.api.auth.key", apiKey);
                put("web.http.port", String.valueOf(getFreePort()));
                put("web.http.path", "/api");
                put("web.http.protocol.port", String.valueOf(protocolEndpoint.getUrl().getPort()));
                put("web.http.protocol.path", protocolEndpoint.getUrl().getPath());
                put("web.http.management.port", String.valueOf(managementEndpoint.getUrl().getPort()));
                put("web.http.management.path", managementEndpoint.getUrl().getPath());
                put("web.http.control.port", String.valueOf(controlEndpoint.getUrl().getPort()));
                put("web.http.control.path", controlEndpoint.getUrl().getPath());
                put("web.http.public.path", "/public");
                put("web.http.public.port", String.valueOf(getFreePort()));
                put("edc.dsp.callback.address", protocolEndpoint.getUrl().toString());
                put("edc.connector.name", name);
                put("edc.dataplane.token.validation.endpoint", "http://token-validation.com");
                put("edc.dpf.selector.url", "http://does-this-matter.com");
                put("edc.huawei.iam.endpoint", IAM_OTC_CLOUD_URL);
            }
        };
    }

    public JsonArray getPolicies() {

        AtomicReference<JsonArray> array = new AtomicReference<>();
        await().atMost(TIMEOUT).untilAsserted(() -> {
            var response = managementEndpoint.baseRequest()
                    .contentType(JSON)
                    .when()
                    .post("/v2/policydefinitions/request")
                    .then()
                    .log().ifError()
                    .statusCode(200)
                    .extract().body().asString();

            array.set(objectMapper.readValue(response, JsonArray.class));
        });

        return array.get();
    }

    public static final class Builder extends Participant.Builder<HuaweiParticipant, Builder> {

        private Builder() {
            super(new HuaweiParticipant());
        }

        public Builder apiKey(String apiKey) {
            this.participant.apiKey = apiKey;
            return this;
        }

        @Override
        public HuaweiParticipant build() {
            super.managementEndpoint(new Endpoint(URI.create("http://localhost:" + getFreePort() + "/api/management"), Map.of("X-Api-Key", participant.apiKey)));
            super.protocolEndpoint(new Endpoint(URI.create("http://localhost:" + getFreePort() + "/protocol")));
            super.build();
            participant.controlEndpoint = new Endpoint(URI.create("http://localhost:" + getFreePort() + "/control"), Map.of());
            return participant;
        }

        @JsonCreator
        public static Builder newInstance() {
            return new Builder();
        }
    }
}
