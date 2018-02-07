package net.lulli.metadao.api;


import net.lulli.metadao.model.SQLDialect;

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
