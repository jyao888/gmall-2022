package com.junyao.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-26 0:49
 */
public class Read {
    public static void main(String[] args) throws IOException {
        //创建客户端连接工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //设置链接属性
        jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200").build());

        //获取链接
        JestClient jestClient = jestClientFactory.getObject();

        //读取ES数据
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"男\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"球,足球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "  , \"aggs\": {\n" +
                "    \"groupByClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\"\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"groupByAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 2\n" +
                "}").addIndex("student").build();

        SearchResult searchResult = jestClient.execute(search);

        //获取命中条数
        System.out.println("total: " + searchResult.getTotal());

        //查看第一条明细数据
//        SearchResult.Hit<Map, Void> firstHit = searchResult.getFirstHit(Map.class);
//        System.out.println("_index: " + firstHit.index);
//        System.out.println("_type: " + firstHit.type);
//        System.out.println("_id "+ firstHit.id);
//        System.out.println("_score: " + firstHit.score);
//        Map source = firstHit.source;
//        for (Object o : source.keySet()) {
//            System.out.println(o + ":" + source.get(o));
//        }

        //获取所有明细数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index: " + hit.index);
            System.out.println("_type: " + hit.type);
            System.out.println("_id " + hit.id);
            System.out.println("_score: " + hit.score);
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o + ":" + source.get(o));
            }
            //获取聚合组数据
            MetricAggregation aggs = searchResult.getAggregations();
            //获取班级聚合组数据
            TermsAggregation groupByClass = aggs.getTermsAggregation("groupByClass");
            List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                System.out.println("key: " + bucket.getKey());
                System.out.println("doc_count: " + bucket.getCount());
                //获取嵌套的年龄聚合组数据
                MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
                System.out.println("value: " + groupByAge.getMax());
            }
            //关闭连接
            jestClient.shutdownClient();

        }
    }
}
