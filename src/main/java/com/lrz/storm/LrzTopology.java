package com.lrz.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lrz.storm.bolt.AnalysisKafkaBolt;
import com.lrz.storm.bolt.DataProcesKafkaBolt;
import com.lrz.storm.bolt.RedisStoreBolt;

/**
 * 
 * @Description:公司内部测试 strom + kafka + zookeeper 简析 交易所实时数据 demo
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月6日 下午4:13:41
 */
public class LrzTopology {
	private static final Logger logger = LoggerFactory.getLogger(LrzTopology.class);
	
	
	private static final String KAFKA_SPOUT_ID = "kafka_spout";
    private static final String KAFKA_ANALYS_BOLT_ID = "kafka_analysis_bolt";
    private static final String DATA_PROCES_BOLT_ID = "data_proces_bolt";
    private static final String TOPIC_NAME = "topic";
    private static final String KAFKA_NAME = "/kafka";

	/**
	 * @param strom
	 *            是由main执行的
	 * @TODO strom 程序只允许有一个 main 方法， 多了会报错
	 */
	public static void main(String[] args) {
		logger.info("开始执行..........");

		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * storm 八种 Grouping </br>
		 * 1）shuffleGrouping（随机分组） </br>
		 * 2）fieldsGrouping（按照字段分组，在这里即是同一个单词只能发送给一个Bolt）</br>
		 * 3）allGrouping（广播发送，即每一个Tuple，每一个Bolt都会收到） </br>
		 * 4）globalGrouping（全局分组，将Tuple分配到task id值最低的task里面） </br>
		 * 5）noneGrouping（随机分派）</br>
		 * 6）directGrouping（直接分组，指定Tuple与Bolt的对应发送关系）</br>
		 * 7）Local or shuffle Grouping </br>
		 * 8）customGrouping （自定义的Grouping）</br>
		 */
		BrokerHosts zkHosts = new ZkHosts("192.168.164.133:2181,192.168.164.134:2181,192.168.164.135:2181/kafka");
		// 初始化配置信息
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, TOPIC_NAME, KAFKA_NAME, UUID.randomUUID().toString());
		// zkServers 列表
		List<String> zkServers = new ArrayList<String>() ;
	    zkServers.add("192.168.164.133");
	    zkServers.add("192.168.164.134");
	    zkServers.add("192.168.164.135");
		spoutConfig.zkServers = zkServers;
//		spoutConfig.zkServers = Arrays.asList("127.0.0.1".split(","));
		// zk 端口
		spoutConfig.zkPort = 2181;
		spoutConfig.socketTimeoutMs = 60 * 1000;
		// 解析数据成 string 类型数据 对接kafkaspout 可以通过 getStringByField("str") 获取 也可以通过
		// (String)getValue(0); 获取
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		KafkaSpout spout = new KafkaSpout(spoutConfig);
		
		/**
		 * 第一步： 消息源Spout是storm的Topology中的消息生产者 KafkaConfig.createKafkaSpout()
		 */
//		builder.setSpout("kafka_spout", new TestWordSpout()	, 5);
		builder.setSpout(KAFKA_SPOUT_ID, spout, 2);

		/**
		 * 第二步：解析 kafka_spout 获取需要的数据 ,shuffleGrouping 是最常用的流分组方式，它随机地分发元组到Bolt上的任务
		 */
		builder.setBolt(KAFKA_ANALYS_BOLT_ID, new AnalysisKafkaBolt(), 5).shuffleGrouping(KAFKA_SPOUT_ID);

		/**
		 * 第三步：进行数据拆分，逻辑运算处理 如果下面有逻辑，继续添加 Bolt 方法进行处理
		 */
		builder.setBolt(DATA_PROCES_BOLT_ID, new DataProcesKafkaBolt(), 2).shuffleGrouping(KAFKA_ANALYS_BOLT_ID);

		/**
		 * 第四步：进行保存数据
		 */
		builder.setBolt("RedisStoreBolt", new RedisStoreBolt(), 2).fieldsGrouping(DATA_PROCES_BOLT_ID,new Fields("data"));
		/**
		 * 第五步：创建Topology任务
		 */
		StormTopology wc = builder.createTopology();

		// 配置参数信息
		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setDebug(false);
		//获取本地测试文件
//		conf.put("wordsFile", "D:\\test.txt");
		if (args != null && args.length > 0) {
			// conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, wc);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		} else {
			// 设置成 toplic 分区数
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("otaTopology", conf, wc);
			// Utils.sleep(1000);
		}

	}
}
