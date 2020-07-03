package edu.northwestern.ssa;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;


public class App {
    public static void main(String[] args) throws IOException {
        downloadWARC();
    }

    public static void downloadWARC() throws IOException{
        // Part 1
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(30)).build())
                .build();

        String key = System.getenv("COMMON_CRAWL_FILENAME");


        if (key == null || key.equals("")) {
            ListObjectsV2Request req = ListObjectsV2Request.builder()
                    .bucket("commoncrawl")
                    .prefix("crawl-data/CC-NEWS/2020/04/")
                    .build();
            ListObjectsV2Response keyList = s3.listObjectsV2(req);
            String objKey;
            long largest = 0;
            for (S3Object k : keyList.contents()){
                objKey = k.key();
                long code = Long.parseLong(objKey.replaceAll("[^0-9]", "").substring(6));
                if (code > largest){
                    key = objKey;
                    largest = code;
                }
            }

        }

        FileReader f = new FileReader("./lastWARC.txt");
        StringBuilder sb = new StringBuilder();
        int c;
        while((c = f.read()) != -1) {
            sb.append((char) c);
        }
        f.close();
        if (!sb.toString().equals(key)){
            FileWriter w = new FileWriter("./lastWARC.txt");
            w.write(key);
            w.close();
            GetObjectRequest s3r = GetObjectRequest.builder().bucket("commoncrawl")
                    .key(key)
                    .build();

//          File f = new File("./data.warc.gz");
            InputStream is = s3.getObject(s3r, ResponseTransformer.toInputStream());
            handleWARC(is);
        }

        s3.close();
    }

    public static void handleWARC(InputStream f) throws IOException {

        ArchiveReader reader = WARCReaderFactory.get("./data.warc.gz", f, true);
        int counter = 0;
        NewsData[] temp = new NewsData[15];

        for (ArchiveRecord r : reader){
            if (!r.getHeader().getMimetype().contains("response")){
                continue;
            }
            String e = parseRecord(r);
//            System.out.println(e.length());
            if (e.contains("\r\n\r\n")) {
                temp[counter] = makeRecord(e, r);
                counter++;
                if (counter == temp.length){
                    ElasticSearch es = new ElasticSearch();
                    es.postDocument(temp);
                    es.close();
                    counter = 0;
                }
            }
        }

        if (counter > 0){
            ElasticSearch es = new ElasticSearch();
            es.postDocument(temp);
            es.close();
        }
    }

    public static String parseRecord(ArchiveRecord rec) throws IOException {
        // Part 2
        int size = (int) rec.getHeader().getContentLength();
        byte[] rawData = new byte[size];
        int offset = 0;
        int i;
        while ((i = rec.read(rawData, offset, Integer.MAX_VALUE)) != -1) {
            if (i==0){continue;}
            offset += i;
        }

        return new String(rawData, StandardCharsets.UTF_8);
    }

    public static NewsData makeRecord(String e, ArchiveRecord rec){
        // Part 3
        NewsData curr_rec = new NewsData(rec.getHeader().getUrl());
        String[] st = e.split("\r\n\r\n", 2);
        curr_rec.setHtml(st[1].replaceAll("\u0000", ""));

        Document doc = Jsoup.parse(curr_rec.getHtml());
        curr_rec.setText(doc.text());
        curr_rec.setTitle(doc.title());
        return curr_rec;
    }
}


