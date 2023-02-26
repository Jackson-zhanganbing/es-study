package com.zab.es;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
@RequestMapping("/es")
public class TestController {

    @Autowired
    private EsClient client;

    @PostMapping("/test")
    public Object testInsert(@RequestBody HashMap<String, Object> map) throws Exception {
        return client.query();
    }




}
