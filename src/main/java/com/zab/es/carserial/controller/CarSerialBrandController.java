package com.zab.es.carserial.controller;


import com.zab.es.entity.ResponseVo;
import com.zab.es.util.ReadTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

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
    private ReadTest readTest;

    @RequestMapping("/init")
    private ResponseVo init(){
        readTest.simpleRead();
        return ResponseVo.success("");
    }
}
