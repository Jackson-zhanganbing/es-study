package com.zab.es.util;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.zab.es.properties.EsProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;

/**
 * 封装es 8.5 客户端工具
 *
 * @author zab
 * @date 2023/2/26 22:51
 */
@Slf4j
@Component
public class EsClient {

    @Autowired
    private EsProperties properties;

    private volatile ElasticsearchClient client;

    public ElasticsearchClient getEsClient() {
        if (client == null) {
            synchronized (EsClient.class) {
                if (client == null) {
                    String fingerprint = properties.getFingerprint();

                    SSLContext sslContext = TransportUtils
                            .sslContextFromCaFingerprint(fingerprint);

                    BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
                    credsProv.setCredentials(
                            AuthScope.ANY, new UsernamePasswordCredentials(properties.getLogin(), properties.getPassword())
                    );

                    SniffOnFailureListener listener = new SniffOnFailureListener();

                    RestClient restClient = RestClient
                            .builder(new HttpHost(properties.getHost(), properties.getPort(), properties.getScheme()))
                            .setHttpClientConfigCallback(hc -> hc
                                    .setSSLContext(sslContext)
                                    .setDefaultCredentialsProvider(credsProv)
                            ).setFailureListener(listener)
                            .build();

                    ElasticsearchNodesSniffer elasticsearchNodesSniffer = new ElasticsearchNodesSniffer(restClient,
                            ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                            ElasticsearchNodesSniffer.Scheme.HTTPS);
                    //sniffer可以嗅探到加入es集群的其他节点
                    //region Description
                    Sniffer sniffer = Sniffer.builder(restClient)
                            .setSniffIntervalMillis(5000)
                            .setSniffAfterFailureDelayMillis(30000)
                            .setNodesSniffer(elasticsearchNodesSniffer)
                            .build();

                    listener.setSniffer(sniffer);
                    //endregion
                    // Create the transport and the API client
                    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                    client = new ElasticsearchClient(transport);
                    return client;
                } else {
                    return client;
                }

            }

        } else {
            return client;
        }
    }

    public void sniffer(){
        ElasticsearchClient esClient = getEsClient();

    }

    public String query() throws IOException {
        SearchResponse<JSONObject> search = getEsClient().search(s -> s
                        .index("area")
                        .query(q -> q
                                .nested(t -> t
                                        .path("province")
                                        .query(p -> p.match(
                                                                  m -> m.field("province.name").query("北京")
                                                )

                                        )
                                )),
                JSONObject.class);

        for (Hit<JSONObject> hit : search.hits().hits()) {
            System.out.println(JSONUtil.toJsonPrettyStr(hit.source()));
            return JSONUtil.toJsonPrettyStr(JSONUtil.toJsonPrettyStr(hit.source()));
        }
        return null;
    }

    public boolean insertOneIndex(String index, JSONObject json) throws IOException {
        //CreateIndexResponse createIndexResponse = getEsClient().indices().create(c -> c.index(index));

        IndexRequest request = IndexRequest.of(i -> i
                .index(index)
                .id(json.getStr("id"))
                .document(json.get("data"))
        );

        IndexResponse response = getEsClient().index(request);

        log.info("Indexed with version " + response.version());
        return true;
    }
}
