package com.zab.es;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
     * es 面向文档搜索，层级关系：index--->type--->id
     * 高版本的es移除type概念
     * 指定index、type和id搜索，index（数据库），type（数据表），id
     * <p>
     * 对应url操作：http://192.168.242.200:9200/caeri/employee/1
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
     * 根据多个id查
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
     * 指定index、type和文档中的某字段 搜索，要注意字段类型，如果是text的用term不行，中文要分词
     */
    @PostMapping("/queryByWhere")
    public Object queryByWhere(@RequestBody HashMap<String, Object> map) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("name", map.get("name").toString()));

        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(Integer.valueOf(map.get("from").toString()));
        searchSourceBuilder.size(Integer.valueOf(map.get("size").toString()));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(map.get("index").toString());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        return hits;
    }


}
