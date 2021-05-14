package com.zab.es;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;

@Configuration
public class EsConfiguration {
	@Value("${es.host}")
	private String hosts = "192.168.242.200";
	@Value("${es.port}")
	private Integer port = 9200;
	@Value("${es.scheme}")
	private String schema = "http";
	private ArrayList<HttpHost> hostList = null;
	 
	private static int connectTimeOut = 1000;
	private static int socketTimeOut = 30000;
	private static int connectionRequestTimeOut = 500;
	 
	private static int maxConnectNum = 100;
	private static int maxConnectPerRoute = 100;
	 
	private ArrayList<HttpHost> getHostList(){
		if(hostList == null) {
			hostList = new ArrayList<>();
			String[] hostStrs = hosts.split(",");
			for (String host : hostStrs) {
				hostList.add(new HttpHost(host, port, schema));
			}
		}
		return hostList;
	}
	 
	@Bean
	public RestHighLevelClient restHighLevelClient() {
		RestClientBuilder builder = RestClient.builder(getHostList().toArray(new HttpHost[0]));
		// 异步httpclient连接延时配置
		builder.setRequestConfigCallback(new RequestConfigCallback() {
			@Override
			public Builder customizeRequestConfig(Builder requestConfigBuilder) {
				requestConfigBuilder.setConnectTimeout(connectTimeOut);
				requestConfigBuilder.setSocketTimeout(socketTimeOut);
				requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
				return requestConfigBuilder;
			}
		});
		// 异步httpclient连接数配置
		builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				httpClientBuilder.setMaxConnTotal(maxConnectNum);
				httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
				return httpClientBuilder;
			}
		});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

}
