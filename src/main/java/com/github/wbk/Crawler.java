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
import java.sql.SQLException;
import java.util.stream.Collectors;

/**
 * @author sentry
 */
public class Crawler extends Thread {
    private ICrawlerDAO dao;

    public Crawler(ICrawlerDAO dao) {
        this.dao = dao;
    }

    @Override
    public void run() {

        try {
            String link;
            while ((link = dao.getNextLinkThenDelete()) != null) {
                if (dao.isProcessedLink(link)) {
                    continue;
                }
                if (isValidLink(link)) {
                    Document doc = getAndParasHtml(link);
                    Elements links = doc.select("a");
                    if (!links.isEmpty()) {
                        parseUrlsAndInsertIntoDatabase(links);
                    }
                    storeIntoDatabaseIfItIsNewsPage(doc, link);
                    dao.insertProcessedLink(link);
                    //dao.updateDatabase(link, "INSERT INTO LINKS_ALREADY_PROCESSED (LINK)VALUES (?)");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void parseUrlsAndInsertIntoDatabase(Elements links) throws SQLException {
        for (Element aTag : links) {
            String href = aTag.attr("href");
            dao.insertLinkToBeProcessed(href);
            //dao.updateDatabase(href, "INSERT INTO LINKS_TO_BE_PROCESSED (LINK)VALUES (?)");

        }
    }


    public void storeIntoDatabaseIfItIsNewsPage(Document doc, String link) throws SQLException {
        Elements articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                System.out.println(title);
                String content = articleTag.select("p").stream().map(Element::text).collect(Collectors
                        .joining("\n"));
                dao.storeContentIntoDatabase(title, link, content);
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

