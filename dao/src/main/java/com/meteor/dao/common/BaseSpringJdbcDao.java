package com.meteor.dao.common;

import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.incrementer.AbstractSequenceMaxValueIncrementer;
import org.springframework.jdbc.support.incrementer.DB2SequenceMaxValueIncrementer;
import org.springframework.jdbc.support.incrementer.OracleSequenceMaxValueIncrementer;


/**
 * Spring的JDBC基类
 *
 */
public abstract class BaseSpringJdbcDao implements InitializingBean {

	protected final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	protected SimpleJdbcTemplate simpleJdbcTemplate;
	@Autowired
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	@Autowired
	protected SimpleJdbcTemplate slaveSimpleJdbcTemplate;
	@Autowired
	protected NamedParameterJdbcTemplate slaveNamedParameterJdbcTemplate;
	
	@Autowired
	protected DataSource masterDataSource;
	
	@Autowired
	protected DataSource slaveDataSource;
	
	
	@Override
	public final void afterPropertiesSet() throws IllegalArgumentException, BeanInitializationException {
	}
	
	public SimpleJdbcTemplate getSimpleJdbcTemplate() {
		return simpleJdbcTemplate;
	}

	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
		return namedParameterJdbcTemplate;
	}

	public SimpleJdbcTemplate getSlaveSimpleJdbcTemplate() {
		return slaveSimpleJdbcTemplate;
	}

	public NamedParameterJdbcTemplate getSlaveNamedParameterJdbcTemplate() {
		return slaveNamedParameterJdbcTemplate;
	}

	protected Class<?> getEntityClass(){
		throw new UnsupportedOperationException("not yet implements");
	}
	
	/**
	 * 适用sqlserver:identity,mysql:auto_increment 自动生成主键
	 */
	public int insertWithGeneratedKey(Object entity, String insertSql) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		int affectedRows = getNamedParameterJdbcTemplate().update(insertSql, new BeanPropertySqlParameterSource(entity) , keyHolder);
		setIdentifierProperty(entity, keyHolder.getKey().longValue());
		return affectedRows;
	}

	public Object insertWithGeneratedKeyReturnObject(Object entity, String insertSql) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		getNamedParameterJdbcTemplate().update(insertSql, new BeanPropertySqlParameterSource(entity) , keyHolder);
		setIdentifierProperty(entity, keyHolder.getKey().longValue());
		return entity;
	}
	
	public int insertWithIdentity(Object entity,String insertSql) {
		return insertWithGeneratedKey(entity, insertSql);
	}
	
	public int insertWithAutoIncrement(Object entity,String insertSql) {
		return insertWithIdentity(entity,insertSql);
	}

	public int insertWithSequence(Object entity,AbstractSequenceMaxValueIncrementer sequenceIncrementer,String insertSql) {
		Long id = sequenceIncrementer.nextLongValue();
		setIdentifierProperty(entity, id);
		return getNamedParameterJdbcTemplate().update(insertSql, new BeanPropertySqlParameterSource(entity));
	}
	
	public int insertWithDB2Sequence(Object entity,String sequenceName,String insertSql) {
		return insertWithSequence(entity, new DB2SequenceMaxValueIncrementer(masterDataSource,sequenceName), insertSql);
	}
	
	public int insertWithOracleSequence(Object entity,String sequenceName,String insertSql) {
		return insertWithSequence(entity, new OracleSequenceMaxValueIncrementer(masterDataSource,sequenceName), insertSql);
	}
	
	public int insertWithUUID(Object entity,String insertSql) {
		String uuid = UUID.randomUUID().toString().replace("-", "");
		setIdentifierProperty(entity, uuid);
		return getNamedParameterJdbcTemplate().update(insertSql, new BeanPropertySqlParameterSource(entity));
	}
	/**
	 * 手工分配ID插入
	 * @param entity
	 * @param insertSql
	 */
	public int insertWithAssigned(Object entity,String insertSql) {
		return getNamedParameterJdbcTemplate().update(insertSql, new BeanPropertySqlParameterSource(entity));
	}
	///// insert with end
	
	/**
	 * 得到主键对应的property
	 */
	protected String getIdentifierPropertyName() {
		throw new UnsupportedOperationException("not yet implements");
	}
	
	/**
	 * 设置实体的主键值
	 */
	public void setIdentifierProperty(Object entity, Object id) {
		try {
			BeanUtils.setProperty(entity, getIdentifierPropertyName(), id);
		} catch (Exception e) {
			throw new IllegalStateException("cannot set property value:"+id+" on entityClass:"+entity.getClass()+" by propertyName:"+getIdentifierPropertyName(),e);
		}
	}
	
	/**
	 * 得到实体的主键值
	 */
	public Object getIdentifierPropertyValue(Object entity) {
		try {
			return PropertyUtils.getProperty(entity, getIdentifierPropertyName());
		} catch (Exception e) {
			throw new IllegalStateException("cannot get property value on entityClass:"+entity.getClass()+" by propertyName:"+getIdentifierPropertyName(),e);
		}
	}
}
