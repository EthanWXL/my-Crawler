package com.github.wbk;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author sentry
 */
public class Main {
    private static final String SITES = "https://sina.cn";

    public static void main(String[] args) throws IOException {
        List<String> linkPool = new ArrayList<>();
        Set<String> processedLinks = new HashSet<>();
        linkPool.add(SITES);
        while (true) {
            if (linkPool.isEmpty()) {
                break;
            }
            String link = linkPool.remove(linkPool.size() - 1);
            if (processedLinks.contains(link)) {
                continue;
            }
            if (isValidLink(link)) {
                Document doc = getAndParasHtml(link);
                Elements links = doc.select("a");
                if (!links.isEmpty()) {
                    for (Element aTag : links) {
                        linkPool.add(aTag.attr("href"));
                    }
                }
                storeIntoDatabaseIfItIsNewsPage(doc);
                processedLinks.add(link);
            }
        }
    }

    private static void storeIntoDatabaseIfItIsNewsPage(Document doc) {
        Elements articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                System.out.println(articleTag.child(0).text());
            }
        }
    }

    private static Document getAndParasHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        if (link.startsWith("//")) {
            link = "https:" + link;
        }
        System.out.println(link);

        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36\n");
        CloseableHttpResponse response = httpclient.execute(httpGet);
        HttpEntity entity1 = response.getEntity();
        String html = EntityUtils.toString(entity1);
        return Jsoup.parse(html);
    }

    private static boolean isValidLink(String link) {
        return link.contains("news.sina.cn") && !link.contains("passport") || "https://sina.cn".equals(link);
    }
}

