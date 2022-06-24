package com.junyao.gmallpublisher.service.impl;

import com.junyao.gmallpublisher.dao.DauMapper;
import com.junyao.gmallpublisher.dao.OrderMapper;
import com.junyao.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-22 14:56
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {
        //1.获取Dao层的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map集合用来存放返回的数据 k：老map中LH对应的值 v：老map中CT对应的值
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list集合获取到老map然后封装新的map

        for (Map map : list) {
            System.out.println(map);
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvHourTotal(String date) {
        //获取Dao层的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //创建Map集合用来存放返回的数据 k->老map中CREATE_HOUR对应的value v->老map中SUM_AMOUNT对应的value
        HashMap<String, Double> result = new HashMap<>();

        //遍历list集合获取到老map然后封装新的map
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
        }
        return result;
    }
}
