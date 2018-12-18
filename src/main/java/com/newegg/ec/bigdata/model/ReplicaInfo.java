package com.newegg.ec.bigdata.model;

import java.util.Arrays;

/**
 * @author：Truman.P.Du
 * @createDate: 2018年12月7日 下午1:08:59
 * @version:1.0
 * @description:
 */
public class ReplicaInfo {
	private String topic;
	private int partition;
	private int[] replicas;

	public ReplicaInfo(String topic, int partition, int[] replicas) {
		this.topic = topic;
		this.partition = partition;
		this.replicas = replicas;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public int[] getReplicas() {
		return replicas;
	}

	public void setReplicas(int[] replicas) {
		this.replicas = replicas;
	}

	@Override
	public String toString() {
		return "ReplicaInfo [topic=" + topic + ", partition=" + partition + ", replicas=" + Arrays.toString(replicas)
				+ "]";
	}
}
