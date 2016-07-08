package com.meteor.model.view.export;

import com.meteor.model.enumtype.FileType;
import com.meteor.model.view.AbstractTaskDepend;

/**
 * Created by Administrator on 2015/11/13.
 */
public class ExportCassandraToHiveTask extends AbstractTaskDepend {

	private static final long serialVersionUID = -8992233120250294305L;
	public final String CLASS = "com.meteor.model.view.export.ExportCassandraToHiveTask";
	
    private String fetchSql;
    private String columns;
    private String loadSql;

    public ExportCassandraToHiveTask() {
        this.setFileType(FileType.Cassandra2Hive.name());
        this.setProgramClass("com.meteor.server.executor.ShellTaskExecutor");
    }
    
    public String getFetchSql() {
        return fetchSql;
    }

    public void setFetchSql(String fetchSql) {
        this.fetchSql = fetchSql;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

	public String getLoadSql() {
		return loadSql;
	}

	public void setLoadSql(String loadSql) {
		this.loadSql = loadSql;
	}
}
