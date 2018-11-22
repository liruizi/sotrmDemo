package com.lrz.storm.spout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 测试类随机生成
 * @Description:
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月22日 下午7:37:51
 */
public class TestWordSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4698318354951455579L;
	SpoutOutputCollector _collector;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void nextTuple() {
		final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
	}
}
