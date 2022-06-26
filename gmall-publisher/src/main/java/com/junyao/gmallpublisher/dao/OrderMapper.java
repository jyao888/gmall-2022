package com.junyao.gmallpublisher.dao;

import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-26 16:38
 */
public interface OrderMapper {
    //获取订单总数数据
    public Double selectOrderAmountTotal(String date);

    //获取订单分时数据
    public List<Map> selectOrderAmountHourMap(String date);
}
