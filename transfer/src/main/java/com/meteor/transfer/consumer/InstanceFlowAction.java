package com.meteor.transfer.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meteor.model.instance.InstanceFlow;
import com.meteor.transfer.tool.ServiceConfigTool;

public class InstanceFlowAction extends ConsumerAction {

	private static Logger logger = LoggerFactory.getLogger(InstanceFlowAction.class);

	public InstanceFlowAction(int batchMaxSize, long batchIntervalMilli) {
		super(batchMaxSize, batchIntervalMilli);
	}

	@Override
	public void exec(List<byte[]> msgList) {
		if (msgList == null || msgList.size() == 0) {
			return;
		}
		List<InstanceFlow> instanceFlowList = new ArrayList<InstanceFlow>();
		for (byte[] msg : msgList) {
			InstanceFlow instance = (InstanceFlow) SerializationUtils.deserialize(msg);
			instanceFlowList.add(instance);
		}
		ServiceConfigTool.instanceFlowService.batchInsert(instanceFlowList);
		logger.info("InstanceFlowSize = " + instanceFlowList.size());
	}
}
