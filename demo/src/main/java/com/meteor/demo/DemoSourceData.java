package com.meteor.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.time.DateFormatUtils;

public class DemoSourceData {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("metadata.broker.list", "kafka1:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		String topic = "demo_heartbeat";
		String[] uidArr = new String[100];
		String[] refArr = new String[] { "baidu", "google", "360" };
		for (int i = 0; i < 100; i++) {
			uidArr[i] = UUID.randomUUID().toString();
		}

		while (true) {
			List<KeyedMessage<String, String>> msgList = new ArrayList<KeyedMessage<String, String>>();
			for (int i = 0; i < 10; i++) {
				String time = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");
				String uid = uidArr[new Random().nextInt(uidArr.length)];
				String ref = refArr[new Random().nextInt(refArr.length)];
				
				String jsonData = "{\"action\":\"heartbeat\","
						+ "\"stime\":\""
						+ time
						+ "\",\"uid\":"
						+ uid
						+ ",\"ref\":\""
						+ ref
						+ "\"}";
				KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, UUID.randomUUID().toString(), jsonData);
				msgList.add(msg);
				System.out.println("\n" + jsonData + "\n");
			}
			producer.send(msgList);
			
			System.out.println("睡眠60s...");
			Thread.sleep(60 * 1000l);
		}
	}

}
