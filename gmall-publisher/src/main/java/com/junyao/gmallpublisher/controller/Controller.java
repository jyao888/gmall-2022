package com.junyao.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.junyao.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

/**
 * @author wjy
 * @create 2022-06-26 17:15
 */
@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getTotal(@RequestParam ("date") String date){
        //创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //创建存放新增日活的list集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",publisherService.getDauTotal(date));

        //创建存放新增设备的map集合
        HashMap<Object, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        //创建存放新增交易额的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getGmvTotal(date));

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSON.toJSONString(result);
    }

    public String getHourTotal(@RequestParam("id") String id,@RequestParam ("date") String date){

        //根据传入的date获取前一天的date
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        //根据id判断获取的是哪个需求的分时数据
        if ("dau".equals(id)){
            //获取经过Service层处理过后的数据
            todayMap = publisherService.getDauHourTotal(date);
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        }else if ("order_amount".equals(id)){
            todayMap = publisherService.getGmvHourTotal(date);
            yesterdayMap = publisherService.getGmvHourTotal(yesterday);
        }
        //4.创建存放最终结果的map集合
        HashMap<String, Map> result = new HashMap<>();
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        return JSONObject.toJSONString(result);
    }


}
