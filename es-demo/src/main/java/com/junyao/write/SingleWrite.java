package com.junyao.write;

import com.junyao.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.HashMap;


/**
 * @author wjy
 * @create 2022-06-25 23:33
 */
public class SingleWrite {
    public static void main(String[] args) throws IOException {
        //创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //设置链接属性
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //获取链接
        JestClient jestClient = jestClientFactory.getObject();

        //1、直接写入数据
//        Index index = new Index.Builder("{\n" +
//                "  \"id\":\"103\",\n" +
//                "  \"name\":\"三国演义\"\n" +
//                "}").index("movie_01").type("movie").id("1003").build();

        //2、可以把数据封装成map传入
//        HashMap<String, String> source = new HashMap<>();
//        source.put("id","104");
//        source.put("name","红楼梦");
//  Index index = new Index.Builder(source).index("movie_01").type("movie").id("1004").build();

        //3、封装成javaBean写入
        Movie movie = new Movie();
        movie.setId("105");
        movie.setName("梁山伯与祝英台");

        Index index = new Index.Builder(movie).index("movie_01").type("movie").id("1005").build();

        jestClient.execute(index);

        //关闭连接
        jestClient.shutdownClient();
    }
}
