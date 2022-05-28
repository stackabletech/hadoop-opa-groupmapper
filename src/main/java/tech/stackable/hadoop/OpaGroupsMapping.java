package tech.stackable.hadoop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.security.GroupMappingServiceProvider;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;

public class OpaGroupsMapping implements GroupMappingServiceProvider {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();

    private final URI opaGroupUri = URI.create("http://localhost:8181/v1/data/app/rbac/get_groups");

    @SuppressWarnings("unused")
    private static class OpaQuery {
        public OpaQueryInput input;
    }

    @SuppressWarnings("unused")
    private static class OpaQueryInput {
        public String user;

        public OpaQueryInput() {
        }

        public OpaQueryInput(String user) {
            this.user = user;
        }


    }

    private static class OpaQueryResult {
        public String decision_id;
        public List<String> result;
    }

    @Override
    public List<String> getGroups(String s) throws IOException {
        OpaQuery query = new OpaQuery();
        query.input = new OpaQueryInput(s);

        byte[] queryJson;
        try {
            queryJson = json.writeValueAsBytes(query);
        } catch (JsonProcessingException e) {
            System.out.println("failed to serialize: " + e.getMessage());
            queryJson = null;
        }
        HttpResponse<String> response;
        try {
            response = httpClient.send(
                    HttpRequest.newBuilder(opaGroupUri).header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(queryJson)).build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IOException("failed to get groups: " + e.getMessage());
        }
        switch (response.statusCode()) {
            case 200:
                break;
            case 404:
                System.out.println("404");
            default:
                System.out.println("error");
        }
        String responseBody = response.body();
        OpaQueryResult result = null;
        try {
            result = json.readValue(responseBody, OpaQueryResult.class);
        } catch (Exception e) {
            throw new IOException("error deserializing answer: " + responseBody);
        }
        if (result.result == null) {
            throw new IOException("result was empty in response: " + result.toString());
        }
        return result.result;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {

    }

    @Override
    public void cacheGroupsAdd(List<String> list) throws IOException {

    }
}
