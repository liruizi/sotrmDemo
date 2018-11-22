package com.lrz.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lrz.storm.util.RedisPoolManager;

import redis.clients.jedis.Jedis;

/**
 * 
 * @Description:获取上一层数据，进行逻辑处理
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月6日 下午6:13:21
 */
public class RedisStoreBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(RedisStoreBolt.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = -7875692750316372708L;
	private OutputCollector collector;

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
		Jedis jedis=RedisPoolManager.createInstance(); 
		jedis.set("", "");
		String ts = input.getStringByField("data");
		String ch = input.getStringByField("ch");
		String symbol = input.getStringByField("symbol");
		logger.info(ts + "___" + ch + "___" + symbol + "___");
	}

	/**
	 * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
	 * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
