package com.junyao.gmallpublisher.dao;

import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-22 14:51
 */
public interface DauMapper {
    //获取日活总数数据 方法名要和sql对应的id一致，这样才可以绑定
    public Integer selectDauTotal(String date);

    //获取日活分时数据
    public List<Map> selectDauTotalHourMap(String date);
}
