package edu.northwestern.ssa;

import org.json.JSONObject;
import software.amazon.awssdk.http.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

public class ElasticSearch extends AwsSignedRestRequest{
    private String host = System.getenv("ELASTIC_SEARCH_HOST");

    public ElasticSearch(){
        super("es");
    }

//    public HttpExecuteResponse createIndex(String index) throws IOException {
//        String path = System.getenv("ELASTIC_SEARCH_NAME") + "/doc";
//        Map<String, String> param = new HashMap<>();
//        param.put("index", index);
//        return restRequest(SdkHttpMethod.POST, host, path, Optional.of(param));
//    }

    public void postDocument(NewsData[] recs) throws IOException{
        String path = System.getenv("ELASTIC_SEARCH_INDEX") + "/_bulk";
        StringBuilder json = new StringBuilder();
        for (NewsData rec : recs) {
            json.append("{\"index\" : {}}\n");
            JSONObject j = new JSONObject();
            j.put("title", rec.getTitle());
            j.put("txt", rec.getText());
            j.put("url", rec.getUrl());
            json.append(j.toString()).append("\n");
        }
        HttpExecuteResponse request = restRequest(SdkHttpMethod.POST, host, path, Optional.empty(), Optional.of(json.toString()));
        request.responseBody().get().close();
    }


}
