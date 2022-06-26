package com.junyao.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-26 0:50
 */
public class ReadByAPI {
    public static void main(String[] args) throws IOException {
        //1、创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2、设置链接属性
        jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200").build());
        //3、获取链接
        JestClient jestClient = jestClientFactory.getObject();

        //通过调用API来写查询语句 技巧：一层方法 一层对象
        //-----------------------------{}--------------------------------
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //-----------------------------bool--------------------------------
        BoolQueryBuilder bool = new BoolQueryBuilder();

        //-----------------------------term--------------------------------
        TermQueryBuilder term = new TermQueryBuilder("sex", "男");

        //-----------------------------filter--------------------------------
        bool.filter(term);

        //-----------------------------match--------------------------------
        MatchQueryBuilder match = new MatchQueryBuilder("favo", "球,足球");

        //-----------------------------must--------------------------------
        bool.must(match);

        //----------------------------query--------------------------------
        searchSourceBuilder.query(bool);

        //-----------------------------terms--------------------------------
        TermsAggregationBuilder terms = AggregationBuilders.terms("groupByClass").field("class_id");

        //-----------------------------max--------------------------------
        MaxAggregationBuilder max = AggregationBuilders.max("groupByAge").field("age");

        //-----------------------------aggs--------------------------------
        searchSourceBuilder.aggregation(terms.subAggregation(max));

        //-----------------------------from--------------------------------
        searchSourceBuilder.from(0);

        //-----------------------------from--------------------------------
        searchSourceBuilder.size(2);

        ///读取ES数据
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        //获取命中条数
        System.out.println("total: "+searchResult.getTotal());

        //获取所有明细数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index: "+hit.index);
            System.out.println("_type: " + hit.type);
            System.out.println("_id: " + hit.id);
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o +":" + source.get(o));
            }
        }
        //获取聚合组数据
        MetricAggregation aggs = searchResult.getAggregations();
        List<TermsAggregation.Entry> buckets = aggs.getTermsAggregation("groupByClass").getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key: "+bucket.getKey());
            System.out.println("doc_count: "+bucket.getCount());
            //获取嵌套的年龄聚合组数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value: "+groupByAge.getMax());
        }

        //关闭链接
        jestClient.shutdownClient();
    }


}
