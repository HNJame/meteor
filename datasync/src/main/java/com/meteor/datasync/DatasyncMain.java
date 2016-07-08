package com.meteor.datasync;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meteor.datasync.executor.Cassandra2HiveExecutor;
import com.meteor.datasync.executor.Hive2CassandraExecutor;
import com.meteor.datasync.executor.Mysql2CassandraExecutor;
import com.meteor.datasync.executor.Mysql2RedisExecutor;
import com.meteor.model.enumtype.ExecStatus;
import com.meteor.model.enumtype.FileType;
import com.meteor.model.instance.InstanceTask;
import com.meteor.model.view.AbstractBase;
import com.meteor.model.view.AbstractTaskDepend;
import com.meteor.model.view.export.ExportCassandraToHiveTask;
import com.meteor.model.view.importcassandra.ImportHiveToCassandraTask;
import com.meteor.model.view.importcassandra.ImportMysqlToCassandraTask;
import com.meteor.model.view.importredis.ImportMysqlToRedisTask;
import com.meteor.task.TaskManager;

public class DatasyncMain {

	private static Logger logger = LoggerFactory.getLogger(DatasyncMain.class);
	private static String[] dateFormatArray = new String[] { "yyyyMMddHHmmss", "yyyy-MM-dd", "yyyyMMdd" };
	private static String instanceTaskTopic = "instance_task";

	public static void main(String[] args) {
		if (args.length != 14) {
			System.err
					.println("DatasyncMain <taskId> <exportStartTime> <exportEndTime> <importStartTime> <importEndTime> <kafkaBrokers> <cassandraHosts> <redisClusterHostPorts> <jdbcDriver> <jdbcUrl> <jdbcUsername> <jdbcPassword> <tmpPath> <instanceFlowId>");
			System.exit(1);
		}
		String kafkaBrokers = "";
		InstanceTask instanceTask = new InstanceTask();
		instanceTask.setInstanceFlowId(args[13]);
		Date curTime = new Date();
		instanceTask.setReadyTime(curTime);
		instanceTask.setStartTime(curTime);
		File tmpDataPathFile = null;
		
		StringBuilder log = new StringBuilder();
		try {
			InetAddress addr = InetAddress.getLocalHost();
			String ip = addr.getHostAddress().toString();
			String hostname = addr.getHostName().toString();
			log.append(hostname).append("\n");
			log.append(ip).append("\n");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		log.append(args[12] + "/" + args[0] + "_" + args[1] + "_" + args[2] + ".log\n");
		
		try {
			Integer taskId = Integer.parseInt(args[0]);
			Date exportStartTime = DateUtils.parseDate(args[1], dateFormatArray);
			Date exportEndTime = DateUtils.parseDate(args[2], dateFormatArray);
			Date importStartTime = DateUtils.parseDate(args[3], dateFormatArray);
			Date importEndTime = DateUtils.parseDate(args[4], dateFormatArray);
			kafkaBrokers = args[5];
			String cassandraHosts = args[6];
			String redisClusterHostPorts = args[7];
			String jdbcDriver = args[8];
			String jdbcUrl = args[9];
			String jdbcUsername = args[10];
			String jdbcPassword = args[11];
			String tmpPath = args[12];
			String tmpDataPath = tmpPath + "/" + taskId + "_" + args[1] + "_" + args[2] + "/";
			tmpDataPathFile = new File(tmpDataPath);
			FileUtils.deleteQuietly(tmpDataPathFile);
			if (!tmpDataPathFile.exists()) {
				tmpDataPathFile.mkdirs();
			}
			logger.info("tmpDataPath: " + tmpDataPath);

			TaskManager taskManager = TaskManager.getInstance(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);
			AbstractBase task = taskManager.getTask(taskId);
			logger.info(task.toString());
			instanceTask.setTask((AbstractTaskDepend) task);

			Map<String, Object> params = new HashMap<String, Object>();
			params.put("exportStartTime", exportStartTime);
			params.put("exportEndTime", exportEndTime);
			params.put("importStartTime", importStartTime);
			params.put("importEndTime", importEndTime);
			params.put("tmpPath", tmpPath);
			params.put("tmpDataPath", tmpDataPath);
			params.put("cassandraHosts", cassandraHosts);
			params.put("redisClusterHostPorts", redisClusterHostPorts);

			switch (FileType.getFileTypeByName(task.getFileType())) {
			case Cassandra2Hive:
				Cassandra2HiveExecutor.exec((ExportCassandraToHiveTask) task, params);
				break;
			case Mysql2Cassandra:
				Mysql2CassandraExecutor.exec((ImportMysqlToCassandraTask) task, params);
				break;
			case Mysql2Redis:
				Mysql2RedisExecutor.exec((ImportMysqlToRedisTask) task, params);
				break;
			case Hive2Cassandra:
				Hive2CassandraExecutor.exec((ImportHiveToCassandraTask) task, params);
				break;
			default:
				break;
			}
			instanceTask.setStatus(ExecStatus.Success.name());
		} catch (Exception e) {
			e.printStackTrace();
			instanceTask.setStatus(ExecStatus.Fail.name());
			log.append(ExceptionUtils.getFullStackTrace(e));
		} finally {
			FileUtils.deleteQuietly(tmpDataPathFile);
			instanceTask.setEndTime(new Date());
			instanceTask.setLog(log.toString());
			KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(instanceTaskTopic, UUID.randomUUID().toString(), SerializationUtils.serialize(instanceTask));
			Producer<String, byte[]> producer = getKafkaProducer(kafkaBrokers);
			producer.send(message);
			double duration = (instanceTask.getEndTime().getTime() - instanceTask.getStartTime().getTime()) / 60000;
			logger.info("Finish! Duration of minute: " + duration);
			producer.close();
		}
	}

	public static Producer<String, byte[]> getKafkaProducer(String kafkaBrokers) {
		Properties props = new Properties();
		props.put("metadata.broker.list", kafkaBrokers);
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
		return producer;
	}
}
