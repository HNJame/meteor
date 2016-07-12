package com.meteor.datasync.executor;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meteor.datasync.plugin.ExportHivePlugin;
import com.meteor.datasync.plugin.ImportCassandraPlugin;
import com.meteor.model.view.importcassandra.ImportHiveToCassandraTask;

public class Hive2CassandraExecutor {

	private static Logger logger = LoggerFactory.getLogger(Hive2CassandraExecutor.class);

	public static void exec(ImportHiveToCassandraTask task, Map<String, Object> params) throws Exception {
		ExportHivePlugin.exec(task, params);
		ImportCassandraPlugin.exec(task, params);
	}
}
