package com.zab.es;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/es")
public class TestController {

    @Autowired
    private RestHighLevelClient client;

    @PostMapping("/testInsert")
    public Object testInsert(@RequestBody HashMap<String, Object> map) throws Exception {

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("first_name", map.get("first_name"))
                .field("last_name", map.get("last_name"))
                .field("age", map.get("age"))
                .field("about", map.get("about"))
                .field("interests", map.get("interests"))
                .endObject();

        IndexRequest indexRequest = new IndexRequest(map.get("index").toString()).id(map.get("id").toString()).source(contentBuilder);
        client.index(indexRequest, RequestOptions.DEFAULT);
        return indexRequest;
    }

    /**
     * es ????????????????????????????????????index--->type--->id
     * ????????????es??????type??????
     * ??????index???type???id?????????index??????????????????type??????????????????id
     * <p>
     * ??????url?????????http://192.168.242.200:9200/caeri/employee/1
     */
    @PostMapping("/querySomeOne")
    public Object querySomeOne(@RequestBody HashMap<String, Object> map) throws Exception {
        GetRequest getRequest = new GetRequest(map.get("index").toString(), map.get("id").toString());
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            return getResponse.getSourceAsMap();
        }
        return null;
    }

    /**
     * ????????????id???
     */
    @PostMapping("/queryByIds")
    public Object queryByIds(@RequestBody HashMap<String, Object> map) throws Exception {
        String index = map.get("index").toString();
        List<String> ids = (List<String>) map.get("ids");
        MultiGetRequest request = new MultiGetRequest();
        ids.stream().forEach(id -> {
            request.add(new MultiGetRequest.Item(index, id));
        });
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * ??????index???type???????????????????????? ??????????????????????????????????????????text??????term????????????????????????
     */
    @PostMapping("/queryByWhere")
    public Object queryByWhere(@RequestBody HashMap<String, Object> map) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //?????????????????????
        //BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //??????????????????????????????
        //boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("name", map.get("name").toString()));
        //??????
        //boolQueryBuilder.filter(QueryBuilders.rangeQuery("age").gt(30));

        //searchSourceBuilder.query(boolQueryBuilder);
        //?????????????????????????????????????????????????????????????????????????????????
        //searchSourceBuilder.query(QueryBuilders.matchQuery("last_name", "Smith"));
        //??????????????????????????????
        //searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("last_name","smith"));
        //????????????
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.requireFieldMatch(true);
//        highlightBuilder.field("about");
//        highlightBuilder.preTags("<span style='color:red'>");
//        highlightBuilder.postTags("</span>");
//        searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("about","rock"));
//        searchSourceBuilder.highlighter(highlightBuilder);
        //????????????
        //searchSourceBuilder.from(Integer.valueOf(map.get("from").toString()));
        //searchSourceBuilder.size(Integer.valueOf(map.get("size").toString()));
        //???????????????????????????
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("all_interests").field("interests");
        //???????????????????????????????????????????????????
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_age").field("age");
        termsAggregationBuilder.subAggregation(avgAggregationBuilder);

        searchSourceBuilder.aggregation(termsAggregationBuilder);


        //??????????????????
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(map.get("index").toString());
        searchRequest.source(searchSourceBuilder);

        //?????????????????????????????????
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //======================?????????????????????======================
        Aggregations aggregations = searchResponse.getAggregations();
        Aggregation aggregation = aggregations.get("all_interests");
        //?????????????????????
        List<? extends Terms.Bucket> buckets = ((Terms) aggregation).getBuckets();
        //???????????????????????????
        for (Terms.Bucket bucket : buckets) {
            //?????????key
            String key = bucket.getKeyAsString();
            //????????????
            long docCount = bucket.getDocCount();
            System.out.println(key + "------->" + docCount);
        }
        //======================?????????????????????======================
        return searchResponse.getHits();
    }


}
