package com.junyao.gmallpublisher.controller;



import com.alibaba.fastjson.JSONObject;
import com.junyao.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-22 14:58
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    /**
     * 封装总数数据
     * @param date
     * @return
     */
    @RequestMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date") String date){
        //从service层获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //创建list集合存放最终数据
        ArrayList<Map> result = new ArrayList<>();

        //创建存放新增日活的map集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //创建存放新增日活的map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        //创建存放新增交易额的map集合
        HashMap<String, Object> amountMap = new HashMap<>();
        amountMap.put("id","order_amount");
        amountMap.put("name","新增交易额");
        amountMap.put("value",publisherService.getGmvTotal(date));

        result.add(dauMap);
        result.add(devMap);
        result.add(amountMap);

        return JSONObject.toJSONString(result);
    }
    @RequestMapping("/realtime-hours")
    public String realHourTotal(@RequestParam("id") String id,@RequestParam("date") String date){

        //根据传入的date获取前一天的date
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //根据传入的id判断是日活数据还是交易额数据
        Map todayMap = null;
        Map yesterdayMap = null;
        if("order_amount".equals(id)){
            todayMap = publisherService.getGmvHourTotal(date);
            yesterdayMap = publisherService.getGmvHourTotal(yesterday);
        }else if ("dau".equals(id)){
            //从service层获取分时数据
            todayMap = publisherService.getDauHourTotal(date);
            //根据日期获取昨天的分时数据
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        }
        //存放存储最终结果的Map集合
        HashMap<String, Map> result = new HashMap<>();
        result.put("today",todayMap);
        result.put("yesterday",yesterdayMap);

        return JSONObject.toJSONString(result);
    }
}
