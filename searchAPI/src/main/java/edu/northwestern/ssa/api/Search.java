package edu.northwestern.ssa.api;

import edu.northwestern.ssa.AwsSignedRestRequest;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Path("/search")
public class Search {

    /** when testing, this is reachable at http://localhost:8080/api/search?query=hello */
    @GET
public Response getMsg(@QueryParam("query") String q, @QueryParam("language") String l, @QueryParam("date") String d, @QueryParam("count") String c, @QueryParam("offset") String i) throws IOException {

        Map<String, String> input = new HashMap<>();
        if (q == null) {return Response.status(400).build();}
        q = q.replace(" ", " AND ");
        String qString = "txt:(" + q + ")";
        if (l != null){
            qString += " AND lang:" + l;
        }
        if (d != null){
            qString += " AND date:" + d;
        }
        input.put("q", qString);
        if (c != null){
            input.put("size", c);
        }
        if (i != null){
            input.put("from", i);
        }
        input.put("track_total_hits", "true");
        AwsSignedRestRequest a = new AwsSignedRestRequest("es");
        HttpExecuteResponse er = a.restRequest(SdkHttpMethod.GET, getParam("ELASTIC_SEARCH_HOST"), getParam("ELASTIC_SEARCH_INDEX") + "/_search", Optional.of(input));
        if (er.httpResponse().statusCode() != 200){
            return Response.status(400).build();
        }
        InputStream init_response = er.responseBody().get();


        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte[] data = new byte[1024];
        int j;
        while((j = init_response.read(data)) != -1){
            bo.write(data, 0, j);
        }
        String str_data = bo.toString("UTF-8");
        JSONObject results = new JSONObject(str_data);
        a.close();

        //construct output
        JSONObject output = new JSONObject();
        JSONObject sub_json = results.getJSONObject("hits");
        JSONArray articles = sub_json.getJSONArray("hits");


        JSONArray article_data = new JSONArray();

        for(int x=0; x < articles.length(); x++){
            article_data.put(articles.getJSONObject(x).getJSONObject("_source"));
        }

        output.put("returned_results", articles.length());
        output.put("total_results", sub_json.getJSONObject("total").getInt("value"));
        output.put("articles", article_data);


        return Response.status(200).type("application/json").entity(output.toString(4))
                // below header is for CORS
                .header("Access-Control-Allow-Origin", "*").build();


    }

    private static String getParam(String paramName) {
        String prop = System.getProperty(paramName);
        return (prop != null)? prop : System.getenv(paramName);
    }
}
