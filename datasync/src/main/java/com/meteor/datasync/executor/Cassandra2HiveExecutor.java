package com.meteor.datasync.executor;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meteor.datasync.plugin.ExportCassandraPlugin;
import com.meteor.datasync.plugin.ImportHivePlugin;
import com.meteor.model.view.export.ExportCassandraToHiveTask;

public class Cassandra2HiveExecutor {

	private static Logger logger = LoggerFactory.getLogger(Cassandra2HiveExecutor.class);

	public static void exec(ExportCassandraToHiveTask task, Map<String, Object> params) throws Exception {
		ExportCassandraPlugin.exec(task, params);
		ImportHivePlugin.exec(task, params);
	}
}
