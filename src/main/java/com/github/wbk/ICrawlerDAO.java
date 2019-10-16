package com.github.wbk;

import java.sql.SQLException;

/**
 * @author sentry
 *
 */
public interface ICrawlerDAO {

    String getNextLinkThenDelete() throws SQLException;

    void updateDatabase( String href, String sql) throws SQLException;

    String getNextLinkFromDatabase(String sql) throws SQLException;

    void storeContentIntoDatabase(String title, String link, String content) throws SQLException;

    boolean isProcessedLink(String link) throws SQLException;

}
