package com.meteor.service;

import java.util.List;

import com.meteor.model.menutree.TreeNode;

/**
 * 业务任务操作逻辑
 * @author liuchaohong
 *
 */
public interface ScheduleService {

	public List<TreeNode> getAll() throws Exception;
}
