package com.junyao.gmallpublisher.dao;

import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-23 22:33
 */
public interface OrderMapper {
    //获取交易额总数数据
    public Double selectOrderAmountTotal(String date);

    //获取交易额分时数据
    public List<Map> selectOrderAmountHourMap(String date);
}
