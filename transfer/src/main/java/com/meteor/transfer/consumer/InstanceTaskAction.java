package com.meteor.transfer.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meteor.model.db.DefFileSys;
import com.meteor.model.instance.InstanceTask;
import com.meteor.model.instance.InstanceTaskDB;
import com.meteor.model.util.ViewDBConverters;
import com.meteor.transfer.tool.ServiceConfigTool;

public class InstanceTaskAction extends ConsumerAction {

	private static Logger logger = LoggerFactory.getLogger(InstanceTaskAction.class);

	public InstanceTaskAction(int batchMaxSize, long batchIntervalMilli) {
		super(batchMaxSize, batchIntervalMilli);
	}

	@Override
	public void exec(List<byte[]> msgList) {
		if (msgList == null || msgList.size() == 0) {
			return;
		}
		List<InstanceTaskDB> instanceTaskDBList = new ArrayList<InstanceTaskDB>();
		for (byte[] msg : msgList) {
			InstanceTask instance = (InstanceTask) SerializationUtils.deserialize(msg);
			InstanceTaskDB instanceDB = new InstanceTaskDB();
			instanceDB.setInstanceFlowId(instance.getInstanceFlowId());
			DefFileSys defFileSys = null;
			try {
				defFileSys = ViewDBConverters.convertToDefFileSys(instance.getTask());
			} catch (Exception e) {
				e.printStackTrace();
			}
			instanceDB.setFileId(defFileSys.getFileId());
			instanceDB.setFileBody(defFileSys.getFileBody());
			instanceDB.setReadyTime(instance.getReadyTime());
			instanceDB.setStartTime(instance.getStartTime());
			instanceDB.setEndTime(instance.getEndTime());
			instanceDB.setStatus(instance.getStatus());
			instanceDB.setRetriedTimes(instance.getRetriedTimes());
			instanceDB.setLog(instance.getLog());
			instanceDB.setPoolActiveCount(instance.getPoolActiveCount());
			instanceDB.setPoolQueueSize(instance.getPoolQueueSize());
			instanceDB.setUpdateTime(defFileSys.getUpdateTime());
			instanceDB.setUpdateUser(defFileSys.getUpdateUser());
			instanceTaskDBList.add(instanceDB);
		}
		ServiceConfigTool.instanceTaskService.batchInsert(instanceTaskDBList);
		logger.info("InstanceTaskSize = " + instanceTaskDBList.size());
	}
}
