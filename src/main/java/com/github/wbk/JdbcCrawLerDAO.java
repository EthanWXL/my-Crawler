package com.github.wbk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcCrawLerDAO implements ICrawlerDAO {
    private static final String USER = "root";
    public static final String PASSWORD = "root";
    private Connection conn;

    public JdbcCrawLerDAO() {
        try {
            this.conn = DriverManager.getConnection("jdbc:h2:file:D:/Software/Java/Intellij/Java_learning/XDML/Crawler/my-Crawler/news", USER, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getNextLinkThenDelete() throws SQLException {
        String link = getNextLinkFromDatabase("SELECT LINK FROM LINKS_TO_BE_PROCESSED");
        if (link != null) {
            updateDatabase(link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK = ?");
        }
        return link;
    }


    private void updateDatabase(String href, String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, href);
            statement.executeUpdate();
        }
    }


    private String getNextLinkFromDatabase(String sql) throws SQLException {
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

    @Override
    public void storeContentIntoDatabase(String title, String link, String content) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement
                ("INSERT INTO NEWS(TITLE,URL,CONTENT,CREATED_AT,MODIFIED_AT)VALUES (?,?,?,now(),now())")) {
            statement.setString(1, title);
            statement.setString(2, link);
            statement.setString(3, content);
            statement.executeUpdate();

        }
    }

    @Override
    public boolean isProcessedLink(String link) throws SQLException {
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

    @Override
    public void insertProcessedLink(String link) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement
                ("INSERT INTO LINKS_ALREADY_PROCESSED (LINK)VALUES (?)")) {
            statement.setString(1, link);
            statement.executeUpdate();
        }

    }

    @Override
    public void insertLinkToBeProcessed(String link) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement
                ("INSERT INTO LINKS_TO_BE_PROCESSED (LINK)VALUES (?)")) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }


}

