/**
 * Project Name:trident-tutorial
 * File Name:SentenceSpout.java
 * Package Name:tutorial.storm.trident.test
 * Date:2018年10月23日上午11:57:13
 * Copyright (c) 2018, baiyujie@gmail.com All Rights Reserved.
 *
*/

package com.lrz.storm.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 
 * @Description:向后端发射tuple数据流
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月6日 下午5:30:17
 */
public class SentenceSpout extends BaseRichSpout {

	// BaseRichSpout是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现

	/**
	 * serialVersionUID:TODO(用一句话描述这个变量表示什么).
	 */
	private static final long serialVersionUID = 6473761587068164028L;
	private SpoutOutputCollector collector;
	private FileReader fileReader;

	/**
	 * declareOutputFields是在IComponent接口中定义的，所有Storm的组件（spout和bolt）都必须实现这个接口
	 * 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 告诉组件发出数据流包含sentence字段
		declarer.declare(new Fields("str"));
	}

	/**
	 * open()方法中是ISpout接口中定义，在Spout组件初始化时被调用。
	 * open()接受三个参数:一个包含Storm配置的Map,一个TopologyContext对象，提供了topology中组件的信息,SpoutOutputCollector对象提供发射tuple的方法。
	 * 在这个例子中,我们不需要执行初始化,只是简单的存储在一个SpoutOutputCollector实例变量。
	 */
	@SuppressWarnings("rawtypes")
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			// 获取创建Topology时指定的要读取的文件路径
			this.fileReader = new FileReader(config.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + config.get("wordFile") + "]");
		}
	}

	/**
	 * nextTuple()方法是任何Spout实现的核心。 Storm调用这个方法，向输出的collector发出tuple。
	 * 在这里,我们只是发出当前索引的句子，并增加该索引准备发射下一个句子。
	 */
	public void nextTuple() {
		// if (index >= sentences.length) {
		// return;
		// }
		// //发送字符串
		// this.collector.emit(new Values(sentences[index]));
		// index++;
		String str;
		// Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			// Read all lines
			while ((str = reader.readLine()) != null) {
				/**
				 * 发射每一行，Values是一个ArrayList的实现
				 */
				this.collector.emit(new Values(str));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		}
	}
}
