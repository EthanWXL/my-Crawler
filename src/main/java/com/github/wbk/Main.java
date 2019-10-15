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
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author sentry
 */
public class Main {

    public static void main(String[] args) throws IOException, SQLException {
        //待处理的连接池
        List<String> linkPool = new ArrayList<>();
        Connection conn = DriverManager.getConnection("jdbc:h2:file:D:/Software/Java/Intellij/Java_learning/XDML/Crawler/my-Crawler/news",
                "root", "root");
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("select link FROM LINKS_TO_BE_PROCESSED");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                linkPool.add(resultSet.getString(1));

            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        //已经处理的连接池
        try {
            statement = conn.prepareStatement("select link FROM LINKS_ALREADY_PROCESSED");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {

                statement = insertIntoDatabase(conn, statement, resultSet.getString(1), "INSERT INTO LINKS_ALREADY_PROCESSED(LINK)VALUES (?)");

            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }

        while (true) {
            if (linkPool.isEmpty()) {
                break;
            }

            String link = linkPool.remove(linkPool.size() - 1);
            statement = insertIntoDatabase(conn, statement, link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK = ?");
            boolean processed = false;
            try {
                statement = conn.prepareStatement("select link FROM LINKS_ALREADY_PROCESSED");
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    processed = false;
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
            if (processed) {
                continue;
            }
            if (isValidLink(link)) {
                Document doc = getAndParasHtml(link);
                Elements links = doc.select("a");
                if (!links.isEmpty()) {
                    for (Element aTag : links) {
                        String href = aTag.attr("href");
                        statement = insertIntoDatabase(conn, statement, href, "INSERT INTO LINKS_TO_BE_PROCESSED(LINK) VALUES(?) ");
                    }
                }
                storeIntoDatabaseIfItIsNewsPage(doc);
                //把已经处理过的链接放入数据库
                statement = insertIntoDatabase(conn, statement, link, "INSERT INTO LINKS_ALREADY_PROCESSED(LINK)VALUES (?)");


            }
        }
    }

    private static PreparedStatement insertIntoDatabase(Connection conn, PreparedStatement statement, String href, String sql) throws SQLException {
        try {
            statement = conn.prepareStatement(sql);
            statement.setString(1, href);
            statement.executeUpdate();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        return statement;
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
        return link.contains("news.sina.cn") && link.contains("detail-") && !link.contains("passport") || "https://sina.cn".equals(link);
    }
}

