package com.zab.es.entity;

import com.alibaba.excel.annotation.ExcelProperty;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
public class CarSerialBrandExcel {
    @ExcelProperty("品牌ID")
    private String masterBrandId;
    @ExcelProperty("品牌名称")
    private String masterBrandName;
    @ExcelProperty("厂商ID")
    private String brandId;
    @ExcelProperty("厂商名称")
    private String brandName;
    @ExcelProperty("车系ID")
    private String seriesId;
    @ExcelProperty("车系名称")
    private String seriesName;
    @ExcelProperty("车型ID")
    private String modelId;
    @ExcelProperty("车型名称")
    private String saleName;
}
