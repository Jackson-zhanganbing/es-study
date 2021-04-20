package com.zab.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;

@RestController
@RequestMapping("/es")
public class TestController {

    @Autowired
    private TransportClient client;

    @PostMapping("/testInsert")
    public Object testInsert(@RequestBody HashMap<String, Object> map) throws Exception {

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("first_name", map.get("first_name"))
                .field("last_name", map.get("last_name"))
                .field("age", map.get("age"))
                .field("about", map.get("about"))
                .field("interests", map.get("interests"))
                .endObject();
        IndexResponse response = client.prepareIndex(map.get("index").toString(), map.get("type").toString(), map.get("id").toString()).setSource(contentBuilder).get();
        return response.status();
    }

    /**
     * es 面向文档搜索，层级关系：index--->type--->id
     * 指定index、type和id搜索，index（数据库），type（数据表），id
     * <p>
     * 对应url操作：http://192.168.91.201:9200/caeri/employee/1
     */
    @PostMapping("/querySomeOne")
    public Object querySomeOne(@RequestBody HashMap<String, Object> map) throws Exception {
        GetResponse documentFields = client.prepareGet(map.get("index").toString(), map.get("type").toString(), map.get("id").toString()).get();
        return documentFields;
    }

    /**
     * 指定index、type和文档中的某字段 搜索
     * <p>
     * 对应url操作：http://192.168.91.201:9200/caeri/employee/_search?q=last_name:Smith
     */
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


}
