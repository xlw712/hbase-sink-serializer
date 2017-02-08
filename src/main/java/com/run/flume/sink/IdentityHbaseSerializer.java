package com.run.flume.sink;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 
 * 
 *          __     __       
 *         /  \~~~/  \    
 *   ,----(     ..    ) 
 *  /      \__     __/   
 * /|         (\  |(
 *^ \   /___\  /\ |   
 *   |__|   |__|-" 
 *
 *@Description: TODO
 *@author Levin    
 *@since JDK 1.7
 *@date 2016年11月30日 上午11:59:05
 */
public class IdentityHbaseSerializer implements HbaseEventSerializer {
	private static final Logger logger = LoggerFactory
			.getLogger(IdentityHbaseSerializer.class);
	private Context context;
	private Map<String, String> headers;
	private byte[] cf;
	/**
	 * 是否开启增量入库
	 */
	private String isaugment;
	/**
	 * rowkey在Headers中的Key
	 */
	private String rowkey;
	/**
	 * 数据中多值分隔符
	 */
	private String separate;
	private final static String CONSTANT_ROW_KEY = "rowkey";
	private final static String CONSTANT_IS_AUGMENT = "isaugment";
	private final static String Constant_separate = "separate";

	@Override
	public void configure(Context context) {
		logger.info("call configure method");
		this.context = context;
		this.rowkey = context.getString(CONSTANT_ROW_KEY, "qqnum");
		this.isaugment = context.getString(CONSTANT_IS_AUGMENT, "false");
		if (!isaugment.equals("true") && !isaugment.equals("false")) {
			throw new FlumeException(" isaugment Argument is not set "
					+ isaugment + "must set true or false !!!");
		}
		this.separate = context.getString(Constant_separate, "&&");
	}

	@Override
	public void configure(ComponentConfiguration conf) {

	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
		this.cf = columnFamily;
		this.headers = event.getHeaders();

	}

	@Override
	public List<Row> getActions() {
		List<Row> actions = new ArrayList<Row>(1);
		if (isRowkey(headers.get(rowkey))) {
			if (isaugment.equals("false")) {
				Put put = new Put(Bytes.toBytes(headers.get(rowkey)));
				headers.remove(rowkey);
				for (Map.Entry<String, String> entry : headers.entrySet()) {
					String key = entry.getKey();
					String value = entry.getValue();
					// 将value拆分出多值
					String[] splitV = value.split(separate);
					// 遍历多值字段
					for (int i = 0; i < splitV.length; i++) {
						String tsStr = extractValue(splitV[i], ":");
						long ts = getTs(tsStr);
						put.addColumn(cf, Bytes.toBytes(key), ts,
								Bytes.toBytes(splitV[i]));
					}
				}
				actions.add(put);
			} else {
				// TODO 现不考虑新增数据已何种方式入库
				logger.info(" AugmentFill Method undefinition ");
			}
		} else {
			logger.error("error rowkey :" + headers.get(rowkey));
		}
		return actions;
	}
	/**f
	 * 判断Rowkey是不是正确
	 * @Title: isRowkey
	 * @Description: TODO
	 * @param rowkey
	 * @return
	 * @author Levin
	 */
	private static boolean isRowkey(String rowkey) {
		boolean isRowkey = true;
		if (null == rowkey || "".equals(rowkey)) {
			isRowkey = false;
		} else {
			isRowkey = rowkey.matches("[0-9]{1,}");
		}
		return isRowkey;
	}
	/**
	 * 生产Ts
	 * 
	 * @param tsStr
	 *            多值字段拆分出的单个实际值,如值为空则代表该字段不存在多值,ts采用当前时间
	 * @return
	 */
	private static long getTs(String tsStr) {
		if (null == tsStr) {
			return new Date().getTime();
		}
		if (tsStr.hashCode() > 0) {
			return tsStr.hashCode();
		} else {
			return Long.highestOneBit(Math.abs(tsStr.hashCode()));
		}
	}

	/**
	 * 提取多值字段单个字段的实际值,用于生产timestmp
	 * 
	 * @param splitV
	 *            原始的多值字符串
	 * @param splitChar
	 *            分隔多值采用的分隔符
	 * @return
	 */
	private static String extractValue(String splitV, String splitChar) {
		if (splitV.lastIndexOf(splitChar) > 0 && !splitV.trim().equals("")) {
			return splitV.substring(0, splitV.lastIndexOf(splitChar));
		} else {
			return null;
		}
	}

	@Override
	public List<Increment> getIncrements() {
		return Lists.newArrayList();
	}

	@Override
	public void close() {

	}
}
