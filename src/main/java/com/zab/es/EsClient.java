package com.zab.es;

import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.Map;

/**
 * 封装es 8.5 客户端工具
 *
 * @author zab
 * @date 2023/2/26 22:51
 */
@Component
public class EsClient {

    @Autowired
    private EsProperties properties;

    private ElasticsearchClient client;

    public ElasticsearchClient getEsClient(){
        if(client == null){
            String fingerprint = properties.getFingerprint();

            SSLContext sslContext = TransportUtils
                    .sslContextFromCaFingerprint(fingerprint);

            BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
            credsProv.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(properties.getLogin(), properties.getPassword())
            );

            RestClient restClient = RestClient
                    .builder(new HttpHost(properties.getHost(), properties.getPort(), "https"))
                    .setHttpClientConfigCallback(hc -> hc
                            .setSSLContext(sslContext)
                            .setDefaultCredentialsProvider(credsProv)
                    )
                    .build();
            // Create the transport and the API client
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);
            return client;
        }else{
            return client;
        }
    }

    public String query() throws IOException {
        SearchResponse<Map> search = getEsClient().search(s -> s
                        .index("area")
                        .query(q -> q
                                .nested(t -> t
                                        .path("province")
                                        .query(p -> p.match(
                                                    m -> m.field("province.name").query("北京")
                                                )

                                        )
                                )),
                Map.class);

        for (Hit<Map> hit: search.hits().hits()) {
            System.out.println(JSONUtil.toJsonPrettyStr(hit.source()));
            return JSONUtil.toJsonPrettyStr(JSONUtil.toJsonPrettyStr(hit.source()));
        }
        return null;
    }
}
