package cn.com.bsfit.frms.engine;

import cn.com.bsfit.frms.obj.AuditObject;
import cn.com.bsfit.frms.obj.AuditResult;
import cn.com.bsfit.frms.obj.Risk;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EngineTest2 {
    private RestTemplate template;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static Logger staticLogger = LoggerFactory.getLogger(EngineTest.class);
    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        template = new RestTemplate(requestFactory);
        List<HttpMessageConverter<?>> converters = new ArrayList<HttpMessageConverter<?>>();
        FastJsonHttpMessageConverter fastjson = new FastJsonHttpMessageConverter();
        fastjson.setFeatures(SerializerFeature.WriteClassName);
        converters.add(fastjson);
        template.setMessageConverters(converters);
    }

    @Test
    public void testEngin() throws InterruptedException {
        String url = "http://127.0.0.1:8080/rs/audit";
        Map<?, ?> map = template.postForObject(url, null, Map.class);
        logger.info("url={}, results={}", url, map);
    }

    public static void main(String[] args) {
        int poolSize = 4;
        PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();
        connMgr.setMaxTotal(poolSize + 1);
        connMgr.setDefaultMaxPerRoute(poolSize);
        CloseableHttpClient httpClient = HttpClients.custom().setConnectionManager(connMgr).build();
        RestTemplate template = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));
        List<HttpMessageConverter<?>> converters = new ArrayList<HttpMessageConverter<?>>();
        FastJsonHttpMessageConverter fastjson = new FastJsonHttpMessageConverter();
        fastjson.setFeatures(SerializerFeature.WriteClassName);
        converters.add(fastjson);
        template.setMessageConverters(converters);

        String url = "http://devhost5:9180/audit";
        Calendar calendar=Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        Date date=calendar.getTime();
        List<AuditObject> audits = new ArrayList<AuditObject>();
        AuditObject auditObject = new AuditObject();
        auditObject.setUuid(UUID.randomUUID().toString());
        auditObject.setUserId("yzq");
        auditObject.setPhoneNo("18868812345");
        auditObject.setBizCode("PAY.BUY");
        auditObject.put("frms_trans_time", date.getTime());
        auditObject.put("frms_pay_type", "qianbao");
        auditObject.put("frms_order_type", "1");
        auditObject.put("frms_create_time", date);
        auditObject.setIpAddr("10.100.1.105");
        auditObject.put("frms_order_id", "2014122318PL479261212");
        auditObject.setTransVol(3000000l);
        auditObject.put("frms_register_mobile_locate", "广西");
        auditObject.put("frms_register_time", date);
        auditObject.put("frms_signed_time", date);
        auditObject.put("frms_acct_id", "yzq");
        auditObject.put("frms_batch_order_id", "2014122318PL479261212");
        List<HashMap<String, Object>>orderlist=new ArrayList<HashMap<String,Object>>();
        HashMap<String, Object>order=new HashMap<String, Object>();
        order.put("frms_goods_name", "电影票");
        order.put("frms_platform_id", "2011102114PT22279640");
        order.put("frms_trans_vol", 3000000l);
        orderlist.add(order);
        auditObject.put("frms_order_list", orderlist);
        audits.add(auditObject);
        List<?> list =  template.postForObject(url, audits, List.class);
        for(Object obj:list){
        	if(obj instanceof AuditResult){
        		AuditResult auditResult=(AuditResult) obj;
        		for(Risk risk:auditResult.getRisks())
        			System.out.println(risk);
        	}
        }
        //staticLogger.info("url={}, results={}", url, list );
    }

}
