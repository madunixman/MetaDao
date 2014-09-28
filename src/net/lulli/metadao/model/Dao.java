package net.lulli.metadao.model;

import net.lulli.metadao.DbConnectionManager;

public abstract class Dao {
	//TODO Singleton
	protected String TABLE_NAME; 
	protected String SQL_DIALECT = SQLDialect.STANDARD; 
	//protected DbConnectionManager dbConnectionmanager;
	public String getSqlDialect() {
		return SQL_DIALECT;
	}
	public void setSqlDialect(String sqlDialect) {
		SQL_DIALECT = sqlDialect;
	}
	
}
