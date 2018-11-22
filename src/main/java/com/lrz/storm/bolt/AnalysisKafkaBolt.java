package com.lrz.storm.bolt;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lrz.storm.util.DateUtil;

/**
 * 
 * @Description:分解kafka 发过来的数据，向下一层传递
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月6日 下午5:01:27
 */
public class AnalysisKafkaBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(AnalysisKafkaBolt.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = -7875692750316372708L;
	private OutputCollector collector;

	/**
	 * kline一秒保持一条记录的缓存.
	 */
	private Map<String, String> klineMap = new HashMap<String, String>();

	/**
	 * 初始化collector
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	/**
	 * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
	 */
	public void execute(Tuple input) {
		String kline = input.getStringByField("str");
		logger.info(kline);
		if (null != kline && !"".equals(kline) && kline.indexOf("{") != -1 && kline.indexOf("}") != -1) {
			JSONObject resJson = JSON.parseObject(kline);
			String ts = resJson.getString("ts");
			String ch = resJson.getString("ch");
			String symbol = ch.split("\\.")[1];
			// 当前记录分钟正点
			try {
				long curTime = DateUtil.getMinute(Long.parseLong(ts));
				if (klineMap.containsKey(symbol)) { // 查看kline缓存中是否存在
					String preRes = klineMap.get(symbol);
					JSONObject preJson = JSON.parseObject(preRes);
					String preTs = preJson.getString("ts");
					long preTime = DateUtil.getMinute(Long.parseLong(preTs));// 上一条记录分钟正点
					if (curTime > preTime) { // 若当前记录分钟正点大于上一条记录分钟正点则认为是一条新记录,则保存上一条记录到文件
						JSONObject preTick = preJson.getJSONObject("tick");
						collector.emit(new Values(ts, ch, symbol, JSON.toJSONString(preTick)));
					}
				}
				klineMap.put(symbol, kline);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
	 * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ts", "ch", "symbol", "tick"));
	}

}
