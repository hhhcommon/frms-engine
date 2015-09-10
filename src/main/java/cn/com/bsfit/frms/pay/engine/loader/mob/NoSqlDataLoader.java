//package cn.com.bsfit.frms.pay.engine.loader.mob;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import org.apache.commons.lang.StringUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//
//import cn.com.bsfit.frms.base.load.EngineLoader;
//import cn.com.bsfit.frms.base.load.LoadTask;
//import cn.com.bsfit.frms.ds.pojo.CacheUser;
//import cn.com.bsfit.frms.obj.AuditObject;
//import cn.com.bsfit.frms.obj.MemCachedItem;
//import cn.com.bsfit.frms.pay.engine.loader.BaseNoSqlLoader;
//import cn.com.bsfit.frms.pay.engine.store.RedisKryoPipelStorer;
//import cn.com.bsfit.frms.sample.pay.pojo.TbIpArea;
//import cn.com.bsfit.frms.sample.pay.pojo.TbMobileRegion;
//import cn.com.bsfit.frms.utils.Globals;
//
//public class NoSqlDataLoader extends BaseNoSqlLoader implements EngineLoader{
//
//    private String bizCode;
//
//    private static Map<String, KeyProperty> keyMap = new LinkedHashMap<String, KeyProperty>(7);
//
//    static {
//        keyMap.put("frms_user_id", new KeyProperty(true, Globals.TAG_ACCTNO));
//        keyMap.put("frms_ip_addr", new KeyProperty(true, Globals.TAG_PAY_IP));
//        keyMap.put("frms_bank_card_no", new KeyProperty(true, Globals.TAG_PAY_CARDNO));
//        keyMap.put("frms_shouhuo_mob", new KeyProperty(true, Globals.TAG_RECEIVE_MOB));
//        keyMap.put("frms_col_user_id", new KeyProperty(true, Globals.TAG_COL_USERID));
//    }
//
//    @Autowired
//    private RedisKryoPipelStorer redisKryoPipelStorer;
//
//    public static class KeyProperty {
//        private String[] tag;
//        private boolean preprocessable = false;
//
//        public String[] getTag() {
//            return tag;
//        }
//
//        public void setTag(String... tag) {
//            this.tag = tag;
//        }
//
//        public boolean isPreprocessable() {
//            return preprocessable;
//        }
//
//        public void setPreprocessable(boolean preprocessable) {
//            this.preprocessable = preprocessable;
//        }
//
//        public KeyProperty(boolean preprocessable, String... tag) {
//            this.tag = tag;
//            this.preprocessable = preprocessable;
//        }
//    }
//
//    private void setCache(AuditObject auditObj){
//        String userId = auditObj.getUserId();
//        if(userId==null||userId.isEmpty()){
//            return;
//        }
//        List<String> userIdList = new ArrayList<>();
//        userIdList.add(userId);
//        Collection<CacheUser> cacheUserList = redisKryoPipelStorer.get(userIdList);
//        if(cacheUserList==null||cacheUserList.isEmpty()){
//            return;
//        }
//        CacheUser cacheUser = ((List<CacheUser>)cacheUserList).get(0);
//        if(cacheUser==null){
//            return;
//        }
//        if(cacheUser.getMobileTime()!=null)
//            auditObj.put("frms_signed_time",cacheUser.getMobileTime());
//        if(cacheUser.getRegisterTime()!=null)
//            auditObj.put("frms_register_time",cacheUser.getRegisterTime());
//        if(cacheUser.getPasswordEditTime()!=null)
//            auditObj.put("frms_password_time",cacheUser.getPasswordEditTime());
//        if(cacheUser.getRegisterIp()!=null)
//            auditObj.put("frms_register_ip",cacheUser.getRegisterIp());
//        if(cacheUser.getMobile()!=null&&cacheUser.getMobile().length()>7){
//        	auditObj.put("frms_register_mobile",cacheUser.getMobile());
//            try{
//                TbMobileRegion mobileData = mobile2City(cacheUser.getMobile().substring(0,7));
//                if(mobileData!=null&&mobileData.getMobileAttr()!=null)
//                    auditObj.put("frms_register_mobile_locate",mobileData.getMobileAttr());
//            }catch (Exception e) {
//                auditObj.put("frms_register_mobile_locate","其他城市");
//            }
//        }
//        
//    }
//
//    private void setColCache(AuditObject auditObj){
//        String userId = auditObj.getUserId();
//        if(userId==null||userId.isEmpty()){
//            return;
//        }
//        List<String> userIdList = new ArrayList<>();
//        userIdList.add(userId);
//        Collection<CacheUser> cacheUserList = redisKryoPipelStorer.get(userIdList);
//        if(cacheUserList==null||cacheUserList.isEmpty()){
//            return;
//        }
//        CacheUser cacheUser = ((List<CacheUser>)cacheUserList).get(0);
//        if(cacheUser==null){
//            return;
//        }
//        if(cacheUser.getMobileTime()!=null)
//            auditObj.put("frms_col_signed_time",cacheUser.getMobileTime());
//        if(cacheUser.getRegisterTime()!=null)
//            auditObj.put("frms_col_register_time",cacheUser.getRegisterTime());
//        if(cacheUser.getPasswordEditTime()!=null)
//            auditObj.put("frms_col_password_time",cacheUser.getPasswordEditTime());
//        if(cacheUser.getRegisterIp()!=null)
//            auditObj.put("frms_col_register_ip",cacheUser.getRegisterIp());
//        if(cacheUser.getMobile()!=null&&cacheUser.getMobile().length()>7){
//        	auditObj.put("frms_col_register_mobile_locate",cacheUser.getMobile());
//            try{
//                TbMobileRegion mobileData = mobile2City(cacheUser.getMobile().substring(0,7));
//                if(mobileData!=null&&mobileData.getMobileAttr()!=null)
//                    auditObj.put("frms_col_register_mobile_locate",mobileData.getMobileAttr());
//            }catch (Exception e) {
//                auditObj.put("frms_col_register_mobile_locate","其他城市");
//            }
//        }
//        
//    }
//    
//    @Override
//    public List<LoadTask> getTask(Object... objects) throws IOException {
//    	if (objects == null || objects.length < 1) {
//            return Collections.emptyList();
//        }
//    	// 批量获取MemcachedItem对象
//        List<LoadTask> loadTaskList = new ArrayList<LoadTask>(1);
//    	final Collection<String> keyList = new HashSet<String>();
//        for (Object obj : objects) {
//            if(obj==null){
//                continue;
//            }
//            if (obj instanceof AuditObject) {
//                AuditObject auditObj = (AuditObject) obj;
//                setCache(auditObj);
//                setColCache(auditObj);
//                //查询ip归属地
//                final String ipAddr = auditObj.getIpAddr();
//                if(ipAddr!=null){
//                    loadTaskList.add(new LoadTask() {
//                        @Override
//                        public List<MemCachedItem> call() throws Exception {
//                            List<MemCachedItem> items = new ArrayList<MemCachedItem>();
//                            TbIpArea tipdata = ip2City(ipAddr);
//                            if(tipdata!=null){
//                            	MemCachedItem item = new MemCachedItem(ipAddr, Globals.TAG_IP_BELONG);
//                                item.put("IP归属地-省", tipdata.getProvince());
//                                item.put("IP归属地-市", tipdata.getCity());
//                                item.put("IP归属地-区", tipdata.getArea());
//                                
//                                MemCachedItem item1 = new MemCachedItem(ipAddr, Globals.TAG_PAY_IP, Globals.BIZ_PAY);
//                                item1.put("IP归属地-省", tipdata.getProvince());
//                                item1.put("IP归属地-市", tipdata.getCity());
//                                item1.put("IP归属地-区", tipdata.getArea());
//                                items.add(item);
//                                items.add(item1);
//                                if (items.size() > 0) {
//                                    return items;
//                                }
//                            }
//                            return null;
//                        }
//                    });                	
//                }
//                keyList.addAll(memCachedKeys(auditObj));
//            }
//        }
//        loadTaskList.add(new LoadTask() {
//            @Override
//            public List<MemCachedItem> call() throws Exception {
//                List<MemCachedItem> items = new ArrayList<MemCachedItem>();
//                items.addAll(getMemCachedItem(keyList));
//                if (items.size() > 0) {
//                    return items;
//                }
//                return null;
//            }
//        });
//        return loadTaskList;
//    }
//    
//    public String[] split(String id) {
//        String[] strs = new String[] { "", "" };
//        if (StringUtils.isNotBlank(id)) {
//            strs = id.split("-");
//        }
//        return strs;
//    }
//
//
//    @Override
//	public Collection<String> memCachedKeys(AuditObject obj) {
//        Collection<String> keys = new HashSet<String>();
//        String bizCode = obj.getBizCode();
//        for (Entry<String, KeyProperty> entry : keyMap.entrySet()) {
//            String id = entry.getKey();
//            String[] idArr = split(id);// 是否为组合维度
//            if (idArr != null && idArr.length >= 2) {
//                Object ob1 = obj.get(idArr[0]);
//                Object ob2 = obj.get(idArr[1]);
//                if (ob1 == null || ob2 == null) {
//                    continue;
//                }
//                String[] strTags = entry.getValue().getTag();
//                for (String strTag : strTags) {
//                    final String realKey = MemCachedItem.getMemCachedKey(ob1.toString() + "-" + ob2.toString(), strTag,
//                            bizCode);
//                    keys.add(realKey);
//                }
//            } else {
//                Object ob = obj.get(id);
//                if (ob == null) {
//                    continue;
//                }
//                String[] strTags = entry.getValue().getTag();
//                for (String strTag : strTags) {
//                    final String realKey = MemCachedItem.getMemCachedKey(ob.toString(), strTag, bizCode);
//                    keys.add(realKey);
//                }
//            }
//        }
//        return keys;
//	}
//
//	public void setBizCode(String bizCode) {
//        this.bizCode = bizCode;
//    }
//
//    public String getBizCode() {
//        return bizCode;
//    }
//
//}
