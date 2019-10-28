package com.github.wbk;

import org.apache.http.HttpHost;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsDataGenerator {
    public static void main(String[] args) {
        SqlSessionFactory sqlSessionFactory;
        String resource = "db/mybatis/config.xml";
        InputStream inputStream;
        try {
            inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<News> newsFromDataBase = getNewsFromDataBase(sqlSessionFactory);

        for (int i = 0; i < 6; i++) {
            new Thread(() -> writeSingleThread(newsFromDataBase)).start();
        }


    }

    private static List<News> getNewsFromDataBase(SqlSessionFactory sqlSessionFactory) {
        List<News> newsFromMysql;
        try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
            newsFromMysql = sqlSession.selectList("com.github.wbk.MockMapper.selectNews");
        }
        return newsFromMysql;
    }

    public static void writeSingleThread(List<News> newsFromMysql) {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")))) {
            //50*2000 = 10_0000
            for (int i = 0; i < 50; i++) {
                BulkRequest bulkRequest = new BulkRequest();
                for (News news : newsFromMysql) {
                    IndexRequest request = new IndexRequest("news");
                    Map<String, Object> map = new HashMap<>();
                    map.put("title", news.getTitle());
                    map.put("url", news.getUrl());
                    map.put("content", news.getContent().length() > 20 ? news
                            .getContent().substring(0, 20) : news.getContent());
                    map.put("createdAt", news.getCreatedAt());
                    map.put("modifiedAt", news.getModifiedAt());

                    request.source(map, XContentType.JSON);
                    bulkRequest.add(request);
                }
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                System.out.println("currentThread: " + Thread.currentThread().getName() + " finishs: "
                        + bulkResponse.status().getStatus());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
