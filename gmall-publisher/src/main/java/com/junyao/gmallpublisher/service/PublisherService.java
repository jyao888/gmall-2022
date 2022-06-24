package com.junyao.gmallpublisher.service;

import com.junyao.gmallpublisher.dao.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-22 14:53
 */

public interface PublisherService {
    //日活总数数据接口
    public Integer getDauTotal(String date);

    //日活分时数据接口
    public Map getDauHourTotal(String date);

    //交易额总数抽象方法
    public Double getGmvTotal(String date);

    //交易额分时数据抽象方法
   public Map getGmvHourTotal(String date);
}
