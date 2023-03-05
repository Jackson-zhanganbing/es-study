package com.zab.es.carserial.controller;


import com.zab.es.carserial.entity.CarSerialBrand;
import com.zab.es.carserial.service.ICarSerialBrandService;
import com.zab.es.entity.ResponseVo;
import com.zab.es.util.CarSerialReader;
import com.zab.es.util.EsClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author zab
 * @since 2023-03-05
 */
@RestController
@RequestMapping("/car-serial-brand")
public class CarSerialBrandController {

    @Autowired
    private CarSerialReader carSerialReader;

    @Autowired
    private EsClient esClient;

    @Autowired
    private ICarSerialBrandService carSerialBrandService;

    @RequestMapping("/initMysql")
    private ResponseVo initMysql(){
        carSerialReader.readCarSerial();
        return ResponseVo.success("");
    }
    @RequestMapping("/initEs")
    private ResponseVo initEs(){
        List<CarSerialBrand> list = carSerialBrandService.list();
        esClient.bulk(list);
        return ResponseVo.success("");
    }
}
