package com.meteor.util;

import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;
/**
 * 数据源提供器
 *
 */
public class DataSourceProvider {
	
	private static Logger logger = Logger.getLogger(DataSourceProvider.class);
	private static Map<String, DataSource> dataSourceCache = new HashMap<String, DataSource>();
	
	
	public static synchronized DataSource getDataSource(String jdbcDriver, String jdbcUrl, String jdbcUsername, String jdbcPassword) {
		String key = jdbcDriver + jdbcUrl + jdbcUsername + jdbcPassword;
		DataSource dataSource = dataSourceCache.get(key);
		if(dataSource == null) {
			try {
				ComboPooledDataSource ds = new ComboPooledDataSource();
				ds.setDriverClass(jdbcDriver);
				
				ds.setJdbcUrl(jdbcUrl);
				ds.setUser(jdbcUsername);
				ds.setPassword(jdbcPassword);
				ds.setCheckoutTimeout(10 * 1000);
				
				if(jdbcDriver.contains("mysql")) {
					ds.setPreferredTestQuery("select 1");
					ds.setTestConnectionOnCheckin(true);
					ds.setTestConnectionOnCheckout(true);
				}
				//设置sql执行超时时间为1分钟，如一个Sql执行时间超过了指定时间，连接池会自动断开此连接
				ds.setUnreturnedConnectionTimeout(60);
				ds.setLoginTimeout(10);
				ds.setMinPoolSize(1);
				ds.setInitialPoolSize(1);
				
				dataSource = ds;
				dataSourceCache.put(key, dataSource);
				logger.info("create DataSource, url:[" + jdbcUrl + "], username:" + jdbcUsername);
			} catch (PropertyVetoException e) {
				throw new IllegalArgumentException("invalid driver:" + jdbcDriver, e);
			} catch( SQLException e) {
				throw new IllegalArgumentException(e);
			}
		}
		return dataSource;
	}
	
}
