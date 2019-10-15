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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sentry
 */
public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:file:D:/Software/Java/Intellij/Java_learning/XDML/Crawler/my-Crawler/news", "root", "root");

        while (true) {
            //待处理的链接池
            List<String> linkPool = getUrlFromDatabase(conn, "SELECT LINK FROM LINKS_TO_BE_PROCESSED");
            //已经处理的连接池
//            Set<String> processedLinks = new HashSet<>(getUrlFromDatabase(conn,"SELECT LINK FROM LINKS_ALREADY_PROCESSED"));
            if (linkPool.isEmpty()) {
                break;
            }
            //处理完成后从池子中删除
            String link = linkPool.remove(linkPool.size() - 1);
            insertIntoDatabase(conn, link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK = ?");
            //询问已处理数据库有没有这个链接
            if (isProcessedLink(conn, link)) {
                continue;
            }
            if (isValidLink(link)) {
                Document doc = getAndParasHtml(link);
                Elements links = doc.select("a");
                if (!links.isEmpty()) {
                    parseUrlsAndInsertIntoDatabase(conn, links);
                }
                storeIntoDatabaseIfItIsNewsPage(doc);
                insertIntoDatabase(conn, link, "INSERT INTO LINKS_ALREADY_PROCESSED (LINK)VALUES (?)");
//                processedLinks.add(link);
            }
        }
    }

    private static void parseUrlsAndInsertIntoDatabase(Connection conn, Elements links) throws SQLException {
        for (Element aTag : links) {
            String href = aTag.attr("href");
            insertIntoDatabase(conn, href, "INSERT INTO LINKS_TO_BE_PROCESSED (LINK)VALUES (?)");

        }
    }

    private static boolean isProcessedLink(Connection conn, String link) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("SELECT LINK FROM LINKS_ALREADY_PROCESSED WHERE LINK = ?")) {
            statement.setString(1, link);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        }
        return false;
    }

    private static void insertIntoDatabase(Connection conn, String href, String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, href);
            statement.executeUpdate();
        }
    }

    private static List<String> getUrlFromDatabase(Connection conn, String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                results.add(resultSet.getString(1));
            }
        }
        return results;
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
        return link.contains("news.sina.cn") && !link.contains("passport")&&link.contains("detail-") || "https://sina.cn".equals(link);
    }
}

