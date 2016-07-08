package com.meteor.service;

import java.util.Date;
import java.util.List;

import com.meteor.model.instance.InstanceTaskDB;
import com.meteor.model.query.InstanceTaskQuery;

public interface InstanceTaskService {
	
	public int[] batchInsert(List<InstanceTaskDB> entityList);
	
	public int cleanHistory(Date minKeepTime);
	
	public List<InstanceTaskDB> getByQuery(InstanceTaskQuery query);

	public InstanceTaskDB getById(String instanceFlowId, Integer fileId);
}
