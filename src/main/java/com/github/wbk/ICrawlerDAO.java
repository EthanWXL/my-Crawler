package com.github.wbk;

import java.sql.SQLException;

/**
 * @author sentry
 *
 */
public interface ICrawlerDAO {

    String getNextLinkThenDelete() throws SQLException;

    void storeContentIntoDatabase(String title, String link, String content) throws SQLException;

    boolean isProcessedLink(String link) throws SQLException;

    void insertProcessedLink(String link) throws SQLException;

    void insertLinkToBeProcessed(String href) throws SQLException;
}
