package com.zab.es.entity;

import lombok.Data;

import java.util.List;

/**
 * 省
 *
 * @author zab
 * @date 2023/2/26 23:04
 */
@Data
public class Province {
    private String name;
    private List<City> cities;
}
