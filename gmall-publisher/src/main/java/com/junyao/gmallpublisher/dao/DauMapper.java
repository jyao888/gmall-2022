package com.junyao.gmallpublisher.dao;

import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-26 16:38
 */
public interface DauMapper {
    //获取日活总数数据
    public Integer selectDauTotal(String date);

    //获取日活分时数据
    public List<Map> selectDauTotalHourMap(String date);
}
