package com.newegg.ec.bigdata.command;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import com.newegg.ec.bigdata.model.ReplicaInfo;
import com.newegg.ec.bigdata.util.FileUtil;
import com.newegg.ec.bigdata.util.ShellUtil;

/**
 * @author：Truman.P.Du
 * @createDate: 2018年12月6日 下午1:51:23
 * @version:1.0
 * @description: kafka leader 重新选举
 * 
 *               1.获取所有topic 需要--zookeeper参数,生成topics.json
 * 
 *               2.根据topic文件生成replica
 *               assignment(reassignment_pattition.json),备份replica
 *               assignment(backup_reassignment_pattition.json)
 *               //需要参数--zookeeper --broker
 *               将原来backup_reassignment_pattition.json中内容取出leader是broker相关信息，将该broker移至末尾，
 * 
 *               3.根据reassignment_pattition.json生成topicPartitionList.json，即将受影响的partition写入该文件
 * 
 *               4.执行kafka-reassign-partitions --zookeeper $zkServer
 *               --reassignment-json-file reassignment.json --execute
 * 
 *               5.执行kafka-preferred-replica-election --zookeeper $zkServer
 *               --path-to-json-file topicPartitionList.json
 */
@ShellComponent
public class KafkaLeaderElection {

	private final String TOPICS_FILE = "topics.json";
	private final String REASSIGNMENT_PATTITION_FILE = "reassignment_pattition.json";
	private final String BACKUP_REASSIGNMENT_PATTITION_FILE = "backup_reassignment_pattition.json";
	private final String TOPICPARTITIONLIST_FILE = "topicPartitionList.json";
	
	private final String BUCKUP_DIR = "./backup";
	
	
	String kafkaReassignPartitions = "kafka-reassign-partitions";
	String kafkaPreferredReplicaElection = "kafka-preferred-replica-election";

	/**
	 * 生成集群中所有topic的json文件
	 * 
	 * @param zookeeper
	 * @throws Exception
	 */
	@ShellMethod(key = "generate-topics", prefix = "--", value = "generate-topics --zookeeper 192.168.0.101:8181")
	public void generateTopics(String zookeeper) throws Exception {
		List<String> topics = null;
		ZkClient zkClient = null;
		try {
			zkClient = new ZkClient(zookeeper, 30000);
			topics = zkClient.getChildren("/brokers/topics");

		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			if (zkClient != null)
				zkClient.close();
		}

		if (topics == null || topics.size() == 0) {
			System.out.println("no topics!");
			return;
		}
		JSONObject json = new JSONObject();
		JSONArray topicsArray = new JSONArray();
		topics.forEach(topic -> {
			JSONObject topicObject = new JSONObject();
			try {
				topicObject.put("topic", topic);
			} catch (JSONException e) {
				e.printStackTrace();
				return;
			}
			topicsArray.put(topicObject);
		});

		json.put("topics", topicsArray);
		json.put("version", 1);

		if (FileUtil.writeFile(TOPICS_FILE, json.toString())) {
			System.out.println("generate " + TOPICS_FILE + " success!");
		}
	}
	/**
	 * 备份当前leader配置信息，生成文件在backup目录下
	 * @param broker
	 * @throws Exception
	 */
	@ShellMethod(key = "backup-leader-info", prefix = "--", value = "backup-leader-info --broker 192.168.0.101:8092")
	public void backupLeaderInfo(String broker) throws Exception {
		List<String> topics = getTopicsList(broker);
		generatorBackup(broker,topics);
	}

	/**
	 * 自动获取所有topics,然后根据该topics,brokerId取出相关partitions，将该信息中的Replicas中指定brokerId移至数组列尾
	 * 
	 * @param broker
	 * @param brokerId
	 * @throws Exception
	 */
	@ShellMethod(key = "reassign-partitions", prefix = "--", value = "reassign-partitions --broker 192.168.0.101:8092 --broker-id 0")
	public void reassignPartitions(String broker, @ShellOption(value = "--broker-id") int brokerId) throws Exception {

		List<String> topics = getTopicsList(broker);
		generator(broker, brokerId, topics);
	}

	/**
	 * 根据topicsJson与brokerId取出相关partitions，将该信息中的Replicas中指定brokerId移至数组列尾
	 * 
	 * @param broker
	 * @param topicsJsonFile
	 * @param brokerId
	 * @throws Exception
	 */
	@ShellMethod(key = "reassign-partitions-by-topics", prefix = "--", value = "reassign-partitions-by-topics --broker 192.168.0.101:8092 --broker-id 0 --topics-json-file topics.json")
	public void reassignPartitions(String broker, @ShellOption(value = "--topics-json-file") String topicsJsonFile,
			@ShellOption(value = "--broker-id") int brokerId) throws Exception {

		if (!FileUtil.existsNotEmpty(topicsJsonFile)) {
			System.out.println(topicsJsonFile + " not exists .");
			return;
		}
		String topicsBuilder = FileUtil.readFile(topicsJsonFile);
		if (topicsBuilder.length() == 0) {
			System.out.println(topicsJsonFile + " is empty.");
			return;
		}

		JSONObject topicsJsons = new JSONObject(topicsBuilder);
		List<String> topics = new ArrayList<String>();
		JSONArray topicsJSONArray = topicsJsons.getJSONArray("topics");
		for (int i = 0; i < topicsJSONArray.length(); i++) {
			topics.add(topicsJSONArray.getJSONObject(i).getString("topic"));
		}
		generator(broker, brokerId, topics);
		tip();
	}
	

	/**
	 * 根据topic与brokerId取出相关partitions，将该信息中的Replicas中指定brokerId移至数组列尾
	 * 
	 * @param broker
	 * @param brokerId
	 * @param topic
	 * @throws Exception
	 */
	@ShellMethod(key = "reassign-partitions-by-topic", prefix = "--", value = "reassign-partitions-by-topic --broker 192.168.0.101:8092 --broker-id 0 --topic kafkatopic")
	public void reassignPartitions(String broker, @ShellOption(value = "--broker-id") int brokerId, String topic)
			throws Exception {

		if (topic.length() == 0) {
			System.out.println(topic + " is empty.");
			return;
		}

		List<String> topics = new ArrayList<String>();
		topics.add(topic);

		generator(broker, brokerId, topics);
		tip();
	}
	/**
	 * 下线所有topic 相应的leader
	 * @param broker
	 * @param zookeeper
	 * @param brokerId
	 * @param pathName
	 * @throws Exception
	 */
	@ShellMethod(key = "cancel-broker-leader", prefix = "--", value = "cancel-broker-leader --broker 192.168.0.101:8092 --zookeeper 192.168.0.101:8181 --broker-id 0 --kafka-install-home /data/bigdata/confluent-3.2.0")
	public void cancelBrokerLeader(String broker, String zookeeper, @ShellOption(value = "--broker-id") int brokerId,
			@ShellOption(value = "--kafka-install-home") String pathName) throws Exception {
		List<String> topics = getTopicsList(broker);
		generatorAndExecuteScript(pathName,broker,brokerId,zookeeper,topics);
	}
	/**
	 * 下线指定topic 相应的leader
	 * @param broker
	 * @param zookeeper
	 * @param brokerId
	 * @param pathName
	 * @param topic
	 * @throws Exception
	 */
	@ShellMethod(key = "cancel-broker-leader-by-topic", prefix = "--", value = "cancel-broker-leader-by-topic --broker 192.168.0.101:8092 --zookeeper 192.168.0.101:8181 --broker-id 0 --topic trumantest --kafka-install-home /data/bigdata/confluent-3.2.0")
	public void cancelBrokerLeaderByTopic(String broker, String zookeeper, @ShellOption(value = "--broker-id") int brokerId,
			@ShellOption(value = "--kafka-install-home") String pathName,String topic) throws Exception {
		List<String> topics = new ArrayList<String>();
		topics.add(topic);
		generatorAndExecuteScript(pathName,broker,brokerId,zookeeper,topics);
	}
	
	/**
	 * 生成相应的json,并执行脚本，实现指定leader下线
	 * @param pathName
	 * @param broker
	 * @param brokerId
	 * @param zookeeper
	 * @param topics
	 */
	private void generatorAndExecuteScript(String pathName,String broker,int brokerId,String zookeeper,List<String> topics ) {
		
		if (pathName.equals("./")) {
			pathName = System.getProperty("user.dir");
		}
		if (!FileUtil.existsNotEmpty(pathName)) {
			System.out.println("kafka-install-home :" + pathName + " not exists.");
			return;
		}
		
		String kafkaReassignPartitionsSh = pathName + "/bin/" + kafkaReassignPartitions;
		String kafkaPreferredReplicaElectionSh = pathName + "/bin/" + kafkaPreferredReplicaElection;

		if (!FileUtil.shExits(kafkaReassignPartitionsSh)) {
			if (!FileUtil.shExits(kafkaReassignPartitionsSh+".sh")) {
				System.out.println(kafkaReassignPartitionsSh + " not exists.");
				return;
			}else {
				kafkaReassignPartitionsSh = kafkaReassignPartitionsSh+".sh";
			}
		}

		if (!FileUtil.shExits(kafkaPreferredReplicaElectionSh)) {
			if (!FileUtil.shExits(kafkaPreferredReplicaElectionSh+".sh")) {
				System.out.println(kafkaPreferredReplicaElectionSh + " not exists.");
				return;
			}else {
				kafkaPreferredReplicaElectionSh = kafkaPreferredReplicaElectionSh+".sh";
			}
		}
		
		generator(broker, brokerId, topics);
		System.out.println("############# kafka-reassign-partitions execute #############");
		System.out.println(ShellUtil.exec(kafkaReassignPartitionsSh+" --zookeeper "+zookeeper+" --reassignment-json-file "+REASSIGNMENT_PATTITION_FILE+" --execute"));
		String verifyResult=ShellUtil.exec(kafkaReassignPartitionsSh+" --zookeeper "+zookeeper+" --reassignment-json-file "+REASSIGNMENT_PATTITION_FILE +" --verify");
		System.out.println(verifyResult);
		while(verifyResult.indexOf("still in progress")>0) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			verifyResult = ShellUtil.exec(kafkaReassignPartitionsSh+" --zookeeper "+zookeeper+" --reassignment-json-file "+REASSIGNMENT_PATTITION_FILE +" --verify");
			System.out.println(verifyResult);
		}
		
		System.out.println("############# kafka-preferred-replica-election execute #############");
		System.out.println(ShellUtil.exec(kafkaPreferredReplicaElectionSh+" --zookeeper "+zookeeper+" --path-to-json-file "+TOPICPARTITIONLIST_FILE));
	}

	private void generator(String broker, int brokerId, List<String> topics) {
		Properties properties = new Properties();
		properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
		AdminClient admin = null;
		try {
			admin = AdminClient.create(properties);
			DescribeTopicsResult describeTopicsResult = admin.describeTopics(topics);
			Map<String, KafkaFuture<TopicDescription>> map = describeTopicsResult.values();
			// partitions原始分配情况
			List<ReplicaInfo> currentPartitions = new ArrayList<ReplicaInfo>();
			// partitions即将修改情况
			List<ReplicaInfo> proposedPartitions = new ArrayList<ReplicaInfo>();
			// 受影响的partition
			List<JSONObject> topicPartitionList = new ArrayList<JSONObject>();

			for (String topicItem : map.keySet()) {
				KafkaFuture<TopicDescription> info = map.get(topicItem);
				try {
					List<TopicPartitionInfo> partitions = info.get().partitions();

					for (TopicPartitionInfo topicPartitionInfo : partitions) {
						// 如果不是指定的brokerId，则不进行处理
						if (topicPartitionInfo.leader().id() != brokerId) {
							continue;
						}
						JSONObject partitionJson = new JSONObject();
						partitionJson.put("topic", topicItem);
						partitionJson.put("partition", topicPartitionInfo.partition());
						topicPartitionList.add(partitionJson);

						List<Node> replicaNodes = topicPartitionInfo.replicas();

						// 构建原始信息

						int[] oldReplicas = new int[replicaNodes.size()];
						for (int j = 0; j < replicaNodes.size(); j++) {
							oldReplicas[j] = replicaNodes.get(j).id();
						}
						int[] replicas = this.fixedStartLocation(brokerId, oldReplicas);
						ReplicaInfo replicaInfo = new ReplicaInfo(topicItem, topicPartitionInfo.partition(), replicas);
						currentPartitions.add(replicaInfo);

						// 调整replicas位置
						int[] updatrReplicas = this.fixedLastLocation(brokerId, oldReplicas);
						ReplicaInfo updateReplicaInfo = new ReplicaInfo(topicItem, topicPartitionInfo.partition(),
								updatrReplicas);
						proposedPartitions.add(updateReplicaInfo);
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			if (proposedPartitions.size() <= 0) {
				System.out.println("no leader in borkerId:" + brokerId);
				return;
			}
			JSONObject currentPartitionsObj = new JSONObject();
			currentPartitionsObj.put("version", 1);
			currentPartitionsObj.put("partitions", currentPartitions);

			JSONObject proposedPartitionsObj = new JSONObject();
			proposedPartitionsObj.put("version", 1);
			proposedPartitionsObj.put("partitions", proposedPartitions);

			JSONObject topicPartitionsObj = new JSONObject();
			topicPartitionsObj.put("partitions", topicPartitionList);
			// 写文件
			if (FileUtil.writeFile(REASSIGNMENT_PATTITION_FILE, proposedPartitionsObj.toString())) {
				System.out.println("generate " + REASSIGNMENT_PATTITION_FILE + " success!");
				if (topics.size() == 1)
					System.out.println(proposedPartitionsObj.toString());
			}
			if (FileUtil.writeFile(BACKUP_REASSIGNMENT_PATTITION_FILE, currentPartitionsObj.toString())) {
				System.out.println("generate " + BACKUP_REASSIGNMENT_PATTITION_FILE + " success!");
				if (topics.size() == 1)
					System.out.println(currentPartitionsObj.toString());
			}
			if (FileUtil.writeFile(TOPICPARTITIONLIST_FILE, topicPartitionsObj.toString())) {
				System.out.println("generate " + TOPICPARTITIONLIST_FILE + " success!");
				if (topics.size() == 1)
					System.out.println(topicPartitionsObj.toString());
			}

		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			if (admin != null)
				admin.close();
		}
	}
	
	/**
	 * 生成当前leader配置信息，通过这两个文件可以将集群leader恢复为当前状况
	 * @param broker
	 * @param topics
	 */
	private void generatorBackup(String broker, List<String> topics) {
		Properties properties = new Properties();
		properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
		AdminClient admin = null;
		try {
			admin = AdminClient.create(properties);
			DescribeTopicsResult describeTopicsResult = admin.describeTopics(topics);
			Map<String, KafkaFuture<TopicDescription>> map = describeTopicsResult.values();
			// partitions原始分配情况
			List<ReplicaInfo> currentPartitions = new ArrayList<ReplicaInfo>();
			// 受影响的partition
			List<JSONObject> topicPartitionList = new ArrayList<JSONObject>();

			for (String topicItem : map.keySet()) {
				KafkaFuture<TopicDescription> info = map.get(topicItem);
				try {
					List<TopicPartitionInfo> partitions = info.get().partitions();

					for (TopicPartitionInfo topicPartitionInfo : partitions) {
						int leaderId = topicPartitionInfo.leader().id();

						JSONObject partitionJson = new JSONObject();
						partitionJson.put("topic", topicItem);
						partitionJson.put("partition", topicPartitionInfo.partition());
						topicPartitionList.add(partitionJson);

						List<Node> replicaNodes = topicPartitionInfo.replicas();
						// 构建原始信息
						int[] oldReplicas = new int[replicaNodes.size()];
						for (int j = 0; j < replicaNodes.size(); j++) {
							oldReplicas[j] = replicaNodes.get(j).id();
						}
						int[] replicas = this.fixedStartLocation(leaderId, oldReplicas);
						ReplicaInfo replicaInfo = new ReplicaInfo(topicItem, topicPartitionInfo.partition(), replicas);
						currentPartitions.add(replicaInfo);
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}


			JSONObject currentPartitionsObj = new JSONObject();
			currentPartitionsObj.put("version", 1);
			currentPartitionsObj.put("partitions", currentPartitions);


			JSONObject topicPartitionsObj = new JSONObject();
			topicPartitionsObj.put("partitions", topicPartitionList);
			// 写文件
			File file = new File(BUCKUP_DIR);
            if(!file.exists()) {
            	file.mkdir();
            }
			if (FileUtil.writeFile(BUCKUP_DIR+"/"+BACKUP_REASSIGNMENT_PATTITION_FILE, currentPartitionsObj.toString())) {
				System.out.println("generate backup " + BACKUP_REASSIGNMENT_PATTITION_FILE + " success!");
				if (topics.size() == 1)
					System.out.println(currentPartitionsObj.toString());
			}
			if (FileUtil.writeFile(BUCKUP_DIR+"/"+TOPICPARTITIONLIST_FILE, topicPartitionsObj.toString())) {
				System.out.println("generate backup" + TOPICPARTITIONLIST_FILE + " success!");
				if (topics.size() == 1)
					System.out.println(topicPartitionsObj.toString());
			}

		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			if (admin != null)
				admin.close();
		}
	}

	private List<String> getTopicsList(String broker) {
		List<String> topics = new ArrayList<String>();

		Properties properties = new Properties();
		properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
		AdminClient admin = null;
		try {
			admin = AdminClient.create(properties);
			ListTopicsResult listTopicsResult = admin.listTopics();
			Set<String> sets = listTopicsResult.names().get();
			topics.addAll(sets);
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			if (admin != null)
				admin.close();
		}

		return topics;
	}

	/**
	 * 将指定数字放在数组第一位，其他按顺序
	 * 
	 * @param num
	 * @param array
	 * @return
	 */
	private int[] fixedStartLocation(int num, int[] array) {
		if (array.length == 1)
			return array;
		int[] replicas = new int[array.length];
		replicas[0] = num;
		int index = 1;
		for (int j = 0; j < array.length; j++) {
			if (index >= array.length)
				break;
			if (array[j] == num) {
				continue;
			}
			replicas[index] = array[j];
			index++;
		}
		return replicas;
	}

	/**
	 * 将指定数字放在数组最后一位，其他按顺序
	 * 
	 * @param num
	 * @param array
	 * @return
	 */
	private int[] fixedLastLocation(int num, int[] array) {
		if (array.length == 1)
			return array;
		int[] replicas = new int[array.length];
		replicas[array.length - 1] = num;
		int index = 0;
		for (int j = 0; j < array.length; j++) {
			if (index >= array.length - 1)
				break;
			if (array[j] == num) {
				continue;
			}
			replicas[index] = array[j];
			index++;
		}
		return replicas;
	}


	
	private void tip() {
		System.out.println("Please manual execute follow step:");
		System.out.println("bin/kafka-reassign-partitions --zookeeper localhost:2181  --reassignment-json-file " + REASSIGNMENT_PATTITION_FILE +" --execute");
		System.out.println("bin/kafka-reassign-partitions --zookeeper localhost:2181  --reassignment-json-file " + REASSIGNMENT_PATTITION_FILE +" --verify");
		System.out.println("kafka-preferred-replica-election execute...");
		System.out.println("bin/kafka-preferred-replica-election --zookeeper localhost:2181 --path-to-json-file " + TOPICPARTITIONLIST_FILE);
	}
	

	public static void main(String[] args) {
		KafkaLeaderElection t = new KafkaLeaderElection();
		/*
		 * List<String> lists = t.getTopicsList("192.168.0.101:8092");
		 * lists.forEach(topic -> { System.out.println(topic); });
		 */

		int[] test1 = { 1 };
		int[] test2 = { 0, 1 };
		int[] test3 = { 0, 1 };
		int[] test4 = { 0, 1, 2, 3 };
		System.out.println("origin test1:" + Arrays.toString(test1));
		System.out.println("origin test2:" + Arrays.toString(test2));
		System.out.println("origin test3:" + Arrays.toString(test3));
		System.out.println("origin test4:" + Arrays.toString(test4));
		System.out.println("-------------------");
		System.out.println(Arrays.toString(t.fixedStartLocation(1, test1)));
		System.out.println(Arrays.toString(t.fixedStartLocation(1, test2)));
		System.out.println(Arrays.toString(t.fixedStartLocation(1, test3)));
		System.out.println(Arrays.toString(t.fixedStartLocation(2, test3)));
		System.out.println(Arrays.toString(t.fixedStartLocation(3, test4)));
		System.out.println("-------------------");
		System.out.println(Arrays.toString(t.fixedLastLocation(1, test1)));
		System.out.println(Arrays.toString(t.fixedLastLocation(1, test2)));
		System.out.println(Arrays.toString(t.fixedLastLocation(0, test3)));
		System.out.println(Arrays.toString(t.fixedLastLocation(1, test4)));

	}
}
