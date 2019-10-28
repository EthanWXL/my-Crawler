package com.github.wbk;

public class Main {
    public static void main(String[] args) {
        ICrawlerDAO dao = new MybatisCrawlerDAO();
//        ICrawlerDAO dao = new JdbcCrawLerDAO();

        for (int i = 0; i < 8; i++) {
            new Crawler(dao).start();
        }

    }
}
