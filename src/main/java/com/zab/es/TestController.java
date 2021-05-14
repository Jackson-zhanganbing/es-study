package com.zab.es;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;

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
     * 指定index、type和id搜索，index（数据库），type（数据表），id
     * <p>
     * 对应url操作：http://192.168.242.200:9200/caeri/employee/1
     */
    @PostMapping("/querySomeOne")
    public Object querySomeOne(@RequestBody HashMap<String, Object> map) throws Exception {
        GetRequest getRequest = new GetRequest(map.get("index").toString(),  map.get("id").toString());
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            return getResponse.getSourceAsMap();
        }
        return null;
    }

    /*  *//**
     * 指定index、type和文档中的某字段 搜索
     * <p>
     * 对应url操作：http://192.168.242.200:9200/caeri/employee/_search?q=last_name:Smith
     *//*
    @PostMapping("/querySimple")
    public Object querySimple(@RequestBody HashMap<String, Object> map) throws Exception {
        // 指定索引和type
        SearchRequestBuilder builder = client.prepareSearch(map.get("index").toString()).setTypes(map.get("type").toString());
        QueryBuilder builder1 = QueryBuilders.matchQuery(map.get("doc_key").toString(), map.get("doc_value").toString());
        builder.setQuery(builder1);
        //执行搜索
        SearchResponse response = builder.execute().actionGet();
        return response;
    }
*/

}
