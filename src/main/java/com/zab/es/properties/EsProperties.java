package com.zab.es.properties;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 用于读取es相关的配置信息
 *
 * @author zab
 * @date 2023/2/26 22:53
 */
@Configuration
@Data
public class EsProperties {
    @Value("${es.host}")
    private String host;
    @Value("${es.port}")
    private Integer port;
    @Value("${es.login}")
    private String login;
    @Value("${es.password}")
    private String password;
    @Value("${es.fingerprint}")
    private String fingerprint;
    @Value("${es.scheme}")
    private String scheme;
}
