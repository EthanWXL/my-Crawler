package com.github.wbk;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Random;

public class MockDataGenerator {

    public static void mockData(SqlSessionFactory sqlSessionFactory, int amount) {

        try (SqlSession sqlsession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            List<News> currentNews = sqlsession.selectList("com.github.wbk.MockMapper.selectNews");
            System.out.println("");
            Random random = new Random();
            int count = amount - currentNews.size();
            try {
                while (count-- > 0) {
                    int i = random.nextInt(currentNews.size());
                    News newsToBeInsert = new News(currentNews.get(i));

                    Instant currentTime = newsToBeInsert.getCreatedAt();
                    currentTime = currentTime.minusMillis(random.nextInt(3600 * 24 * 365));
                    newsToBeInsert.setCreatedAt(currentTime);
                    newsToBeInsert.setModifiedAt(currentTime);

                    sqlsession.insert("com.github.wbk.MockMapper.insertNews", newsToBeInsert);
                    System.out.println("left:" + count);
                    if (count%2000 == 0){
                        sqlsession.flushStatements();
                    }
                }
            }catch (Exception e){
                sqlsession.rollback();
                throw new RuntimeException(e);
            }
            sqlsession.commit();

        }
    }


    public static void main(String[] args) {
        SqlSessionFactory sqlSessionFactory;
        String resource = "db/mybatis/config.xml";
        InputStream inputStream;
        try {
            inputStream = Resources.getResourceAsStream(resource);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        mockData(sqlSessionFactory, 100_0000);
    }
}
