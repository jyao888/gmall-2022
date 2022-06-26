package com.junyao.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author wjy
 * @create 2022-06-26 16:44
 */
@Service
public interface PublisherService {
    //日活总数数据接口
    public Integer getDauTotal(String date);

    //日活分时数据接口
    public Map getDauHourTotal(String date);

    //交易额总数数据接口
    public Double getGmvTotal(String date);

    //交易额分时数据接口
    public Map getGmvHourTotal(String date);
}
