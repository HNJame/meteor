package com.meteor.dao;

import java.util.Date;
import java.util.List;

import com.meteor.model.instance.InstanceFlow;
import com.meteor.model.query.InstanceFlowQuery;

public interface InstanceFlowDao {

	public int[] batchInsert(List<InstanceFlow> entityList);

	public int cleanHistory(Date minKeepTime);

	public List<InstanceFlow> getByQuery(InstanceFlowQuery query);
}
