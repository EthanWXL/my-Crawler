package com.github.wbk;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("https://sina.cn");
        CloseableHttpResponse response = httpclient.execute(httpGet);
        try {

            System.out.println(response.getStatusLine());
            HttpEntity entity1 = response.getEntity();
            System.out.println(EntityUtils.toString(entity1));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            response.close();
        }

    }
}
