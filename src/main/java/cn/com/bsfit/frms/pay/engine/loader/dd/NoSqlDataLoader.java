package cn.com.bsfit.frms.pay.engine.loader.dd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import cn.com.bsfit.frms.base.load.LoadTask;
import cn.com.bsfit.frms.dd.pojo.Bkhqy;
import cn.com.bsfit.frms.dd.pojo.Pgycs;
import cn.com.bsfit.frms.obj.AuditObject;
import cn.com.bsfit.frms.obj.MemCachedItem;
import cn.com.bsfit.frms.pay.engine.loader.BaseNoSqlLoader;
import cn.com.bsfit.frms.utils.Globals;

/**
 * 丹东银行Loader
 * 
 * @author 晨
 *
 */
public class NoSqlDataLoader extends BaseNoSqlLoader {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@SuppressWarnings("rawtypes")
	@Autowired
	@Qualifier("pgycsRedisTemplate")
	protected RedisTemplate redisTemplatePgycs;
	
	@SuppressWarnings("rawtypes")
	@Autowired
	@Qualifier("bkhqyRedisTemplate")
	protected RedisTemplate redisTemplateBkhqy;

	private static Map<String, KeyProperty> keyMap = new LinkedHashMap<String, KeyProperty>(7);

	static {
		keyMap.put("frms_user_id_card", new KeyProperty(true, Globals.TAG_ID_CARD));
		keyMap.put("frms_company_name", new KeyProperty(true, Globals.TAG_COMPANY_NAME));
		keyMap.put("frms_bank_card_no", new KeyProperty(true, Globals.TAG_BANK_CARD_NO, Globals.TAG_CREDIT_CARD_NO));
		keyMap.put("frms_user_name-frms_col_name", new KeyProperty(true, Globals.TAG_TRANS_PARTER));
		keyMap.put("frms_oper_no", new KeyProperty(true, Globals.TAG_TELLER_ID));
		keyMap.put("frms_fre_no", new KeyProperty(true, Globals.TAG_FRENUM));
		//keyMap.put("frms_bank_card_no", new KeyProperty(true, Globals.TAG_CREDIT_CARD_NO));
		keyMap.put("frms_merch_name", new KeyProperty(true, Globals.TAG_CREDIT_MERCH_NAME));
		keyMap.put("frms_bank_card_no-frms_merch_name", new KeyProperty(true, Globals.TAG_CREDIT_CARD_NO_MERCH_NAME));
		// TODO 添加其他字段
	}

	public static class KeyProperty {
		private String[] tag;
		private boolean preprocessable = false;

		public String[] getTag() {
			return tag;
		}

		public void setTag(String... tag) {
			this.tag = tag;
		}

		public boolean isPreprocessable() {
			return preprocessable;
		}

		public void setPreprocessable(boolean preprocessable) {
			this.preprocessable = preprocessable;
		}

		public KeyProperty(boolean preprocessable, String... tag) {
			this.tag = tag;
			this.preprocessable = preprocessable;
		}
	}

	@Override
	public List<LoadTask> getTask(Object... objects) throws IOException {
		if (objects == null || objects.length < 1) {
			return Collections.emptyList();
		}
		// 批量获取MemcachedItem对象
		List<LoadTask> loadTaskList = new ArrayList<LoadTask>(5);
		final Collection<String> keyList = new HashSet<String>();
		for (Object obj : objects) {
			if (obj == null) {
				continue;
			}
			if (obj instanceof AuditObject) {
				AuditObject auditObj = (AuditObject) obj;
				// 设置交易柜员姓名
				setOperName(auditObj);
				setSignDate(auditObj);
				keyList.addAll(memCachedKeys(auditObj));
			}
		}
		loadTaskList.add(new LoadTask() {
			@Override
			public List<MemCachedItem> call() throws Exception {
				List<MemCachedItem> items = new ArrayList<MemCachedItem>();
				items.addAll(getMemCachedItem(keyList));
				if (items.size() > 0) {
					return items;
				}
				return null;
			}
		});
		return loadTaskList;
	}

	/**
	 * 设置用户签约时间
	 * 
	 * @param auditObj
	 */
	@SuppressWarnings("unchecked")
	private void setSignDate(AuditObject auditObj) {
		Object cardNoObj = auditObj.get("frms_bank_card_no");
		if (cardNoObj != null && cardNoObj instanceof String) {
			String cardNo = (String) cardNoObj;
			final String keyNet = "Bkhqy_" + cardNo + "0014";
			final String keyMob = "Bkhqy_" + cardNo + "0019";

			Bkhqy netSign = (Bkhqy) redisTemplateBkhqy.execute(new RedisCallback<Bkhqy>() {

				@Override
				public Bkhqy doInRedis(RedisConnection connection) throws DataAccessException {
					try {
						/*Object aa = redisTemplateBkhqy.getValueSerializer().deserialize(
								connection.get(redisTemplateBkhqy.getKeySerializer().serialize(keyNet)));
						if (aa instanceof Bkhqy) {
							return (Bkhqy)aa;
						}else  {
							return null;
						}*/
						return (Bkhqy) redisTemplateBkhqy.getValueSerializer().deserialize(
								connection.get(redisTemplateBkhqy.getKeySerializer().serialize(keyNet)));
						
					} catch (Exception ex) {
						ex.printStackTrace();
						return null;
					}
				}
			});

			Bkhqy mobSign = (Bkhqy) redisTemplateBkhqy.execute(new RedisCallback<Bkhqy>() {

				@Override
				public Bkhqy doInRedis(RedisConnection connection) throws DataAccessException {
					try {
						/*Object aa = redisTemplateBkhqy.getValueSerializer().deserialize(
								connection.get(redisTemplateBkhqy.getKeySerializer().serialize(keyMob)));
						if (aa instanceof Bkhqy) {
							return (Bkhqy)aa;
						}else  {
							return null;
						}*/
						return (Bkhqy) redisTemplateBkhqy.getValueSerializer().deserialize(
								connection.get(redisTemplateBkhqy.getKeySerializer().serialize(keyMob)));
					} catch (Exception ex) {
						ex.printStackTrace();
						return null;
					}
				}
			});

			if (netSign != null) {
				auditObj.put("frms_sign_date_net", netSign.getKaitrq());
			}
			if (mobSign != null) {
				auditObj.put("frms_sign_date_mob", netSign.getKaitrq());
				auditObj.put("frms_sign_mob_num", netSign.getXnggbh());// 手机号
			}
		}

	}

	/**
	 * 设置交易员姓名
	 * 
	 * @param auditObj
	 */
	@SuppressWarnings("unchecked")
	private void setOperName(AuditObject auditObj) {

		Object operId = auditObj.get("frms_oper_no");
		if (operId != null && operId instanceof String) {
			String operIdStr = (String) operId;
			final String pgycsKey = "Pgycs_" + operIdStr;

			Pgycs pgycsPojo = (Pgycs) redisTemplatePgycs.execute(new RedisCallback<Pgycs>() {

				@Override
				public Pgycs doInRedis(RedisConnection connection) throws DataAccessException {
					try {
						return (Pgycs) redisTemplatePgycs.getValueSerializer().deserialize(
								connection.get(redisTemplatePgycs.getKeySerializer().serialize(pgycsKey)));
					} catch (Exception ex) {
						ex.printStackTrace();
						return null;
					}
				}
			});

			if (pgycsPojo != null) {
				auditObj.put("frms_oper_name", pgycsPojo.getGuiyxm());
			} else {
				logger.error("没有找到交易员信息pojo，交易员id{}", operIdStr);
			}
		}
	}

	public Collection<String> memCachedKeys(AuditObject obj) {
		Collection<String> keys = new HashSet<String>();
		String bizCode = obj.getBizCode();
		for (Entry<String, KeyProperty> entry : keyMap.entrySet()) {
			String id = entry.getKey();
			String[] idArr = split(id);// 是否为组合维度
			if (idArr != null && idArr.length >= 2) {
				Object ob1 = obj.get(idArr[0]);
				Object ob2 = obj.get(idArr[1]);
				if (ob1 == null || ob2 == null) {
					continue;
				}
				String[] strTags = entry.getValue().getTag();
				for (String strTag : strTags) {
					final String realKey = MemCachedItem.getMemCachedKey(ob1.toString() + "-" + ob2.toString(), strTag, bizCode);
					keys.add(realKey);
				}
			} else {
				Object ob = obj.get(id);
				if (ob == null) {
					continue;
				}
				String[] strTags = entry.getValue().getTag();
				for (String strTag : strTags) {
					final String realKey = MemCachedItem.getMemCachedKey(ob.toString(), strTag, bizCode);
					keys.add(realKey);
				}
			}
		}
		return keys;
	}

	public String[] split(String id) {
		String[] strs = new String[] { "", "" };
		if (StringUtils.isNotBlank(id)) {
			strs = id.split("-");
		}
		return strs;
	}

	@Override
	public String getBizCode() {
		// useless
		return Globals.BIZ_CASH;
	}
}
