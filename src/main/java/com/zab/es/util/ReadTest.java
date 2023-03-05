package com.zab.es.util;

import java.io.File;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.read.metadata.ReadSheet;

import com.zab.es.carserial.service.ICarSerialBrandService;
import com.zab.es.entity.CarSerialBrandExcel;
import com.zab.es.excel.CarSerialBrandListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 读的常见写法
 *
 * @author Jiaju Zhuang
 */
@Slf4j
@Component
public class ReadTest {

    @Autowired
    private ICarSerialBrandService carSerialBrandService;

    /**
     * 最简单的读
     */
    public void simpleRead() {

        // 写法4
        String fileName = TestFileUtil.getPath() + File.separator + "demo.xlsx";
        log.info("fileName:{}",fileName);
        // 一个文件一个reader
        try (ExcelReader excelReader = EasyExcel.read(fileName, CarSerialBrandExcel.class, new CarSerialBrandListener(carSerialBrandService)).build()) {
            // 构建一个sheet 这里可以指定名字或者no
            ReadSheet readSheet = EasyExcel.readSheet(0).build();
            // 读取一个sheet
            excelReader.read(readSheet);
        }
    }


}
