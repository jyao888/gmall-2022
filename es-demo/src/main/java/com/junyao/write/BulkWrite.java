package com.junyao.write;

import com.junyao.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author wjy
 * @create 2022-06-26 0:23
 */
public class BulkWrite {
    public static void main(String[] args) throws IOException {
        //1、创建连接工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2、设置链接属性
        jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200").build());

        //3、获取链接
        JestClient jestClient = jestClientFactory.getObject();

        //可以将数据存到一个集合中 然后遍历里面的数据 再通过for循环将数据封装到对应的javaBean里面,再把这些javaBean对象放到集合里面，最后调用execute批量写入
        Movie movie105 = new Movie("106", "鹿鼎记");
        Movie movie106 = new Movie("107", "复仇者联盟");
        Movie movie107 = new Movie("108", "蝙蝠侠");
        Movie movie108 = new Movie("109", "蜘蛛侠");
        Movie movie109 = new Movie("110", "闪电侠");

        Index index105 = new Index.Builder(movie105).id("1006").build();
        Index index106 = new Index.Builder(movie106).id("1007").build();
        Index index107 = new Index.Builder(movie107).id("1008").build();
        Index index108 = new Index.Builder(movie108).id("1009").build();
        Index index109 = new Index.Builder(movie109).id("1010").build();


        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_01")
                .defaultType("movie")
                .addAction(index105)
                .addAction(index106)
                .addAction(index107)
                .addAction(index108)
                .addAction(index109)
                .build();
        //4.批量写入数据
        jestClient.execute(bulk);
        jestClient.shutdownClient();

    }
}
