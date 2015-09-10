package cn.com.bsfit.frms.engine;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import cn.com.bsfit.frms.obj.AuditObject;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;

public class EngineTest {

    public static void main(String[] args) {
        int poolSize = 4;
        PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();
        connMgr.setMaxTotal(poolSize + 1);
        connMgr.setDefaultMaxPerRoute(poolSize);
        CloseableHttpClient httpClient = HttpClients.custom().setConnectionManager(connMgr).build();
        RestTemplate template = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));
        List<HttpMessageConverter<?>> converters = new ArrayList<HttpMessageConverter<?>>();
        FastJsonHttpMessageConverter fastjson = new FastJsonHttpMessageConverter();
        fastjson.setFeatures(SerializerFeature.WriteClassName, SerializerFeature.BrowserCompatible, SerializerFeature.DisableCircularReferenceDetect);
        converters.add(fastjson);
        template.setMessageConverters(converters);

        String url = "http://localhost:9180/audit";
        List<AuditObject> audits = new ArrayList<AuditObject>();
        AuditObject ao = new AuditObject();
        ao.put("frms_acct_id", "yzq");
        ao.setUserId("yzq");
        ao.setBizCode("PAY.BUY");
        ao.put("frms_pay_type","kuaijie");
        ao.put("frms_order_type","1");
        ao.setIpAddr("122.224.121.146");
        ao.put("frms_batch_order_id","2014122318PL47926178");
        ao.setTransVol(3200000L);
        ao.setTransTime(new Date());
        ao.put("frms_order_id","2014122318PL47926178");

        List<HashMap<String, Object>> auditList = new ArrayList<HashMap<String, Object>>();
        HashMap<String, Object> auditMap = new HashMap<String, Object>();
        auditMap.put("frms_goods_name", "Q币_1元直充");
        auditMap.put("frms_platform_id", "2011102114PT22279640");
        auditMap.put("frms_trans_vol",45555000L);
        auditList.add(auditMap);
        ao.put("frms_order_list", auditList);

        audits.add(ao);
        System.out.println(template.postForObject(url, audits, List.class));;
    }
}
