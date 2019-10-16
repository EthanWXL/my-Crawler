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
import java.util.stream.Collectors;

/**
 * @author sentry
 */
public class Main {

    private static final String USER = "root";
    public static final String PASSWORD = "root";

    public static void main(String[] args) throws IOException, SQLException {

        Connection conn = DriverManager.getConnection("jdbc:h2:file:D:/Software/Java/Intellij/Java_learning/XDML/Crawler/my-Crawler/news", USER, PASSWORD);
        String link;
        while ((link = getNextLinkThenDelete(conn)) != null) {
            if (isProcessedLink(conn, link)) {
                continue;
            }
            if (isValidLink(link)) {
                Document doc = getAndParasHtml(link);
                Elements links = doc.select("a");
                if (!links.isEmpty()) {
                    parseUrlsAndInsertIntoDatabase(conn, links);
                }
                storeIntoDatabaseIfItIsNewsPage(conn, doc, link);
                updateDatabase(conn, link, "INSERT INTO LINKS_ALREADY_PROCESSED (LINK)VALUES (?)");
            }
        }
    }

    private static String getNextLinkThenDelete(Connection conn) throws SQLException {
        String link = getNextLinkFromDatabase(conn, "SELECT LINK FROM LINKS_TO_BE_PROCESSED");
        if (link != null) {
            updateDatabase(conn, link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK = ?");
        }
        return link;
    }

    private static void parseUrlsAndInsertIntoDatabase(Connection conn, Elements links) throws SQLException {
        for (Element aTag : links) {
            String href = aTag.attr("href");
            updateDatabase(conn, href, "INSERT INTO LINKS_TO_BE_PROCESSED (LINK)VALUES (?)");

        }
    }

    private static boolean isProcessedLink(Connection conn, String link) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = conn.prepareStatement("SELECT LINK FROM LINKS_ALREADY_PROCESSED WHERE LINK = ?")) {
            statement.setString(1, link);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }

    private static void updateDatabase(Connection conn, String href, String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, href);
            statement.executeUpdate();
        }
    }

    private static String getNextLinkFromDatabase(Connection conn, String sql) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return null;
    }

    private static void storeIntoDatabaseIfItIsNewsPage(Connection conn, Document doc, String link) throws SQLException {
        Elements articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                System.out.println(title);
                String content = articleTag.select("p").stream().map(Element::text).collect(Collectors
                        .joining("\n"));
                try (PreparedStatement statement = conn.prepareStatement
                        ("INSERT INTO NEWS(TITLE,URL,CONTENT,CREATED_AT,MODIFIED_AT)VALUES (?,?,?,now(),now())")) {
                    statement.setString(1, title);
                    statement.setString(2, link);
                    statement.setString(3, content);
                    statement.executeUpdate();

                }

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

