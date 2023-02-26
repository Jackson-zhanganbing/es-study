package com.zab.es.entity;

import lombok.Data;

import java.util.List;

/**
 * 市
 *
 * @author zab
 * @date 2023/2/26 23:03
 */
@Data
public class City {
    private String name;
    private List<District> districts;
}
