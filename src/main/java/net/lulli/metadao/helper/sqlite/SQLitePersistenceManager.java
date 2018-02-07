package net.lulli.metadao.helper.sqlite;

import net.lulli.metadao.DbConnectionManager;
import net.lulli.metadao.MetaPersistenceManager;
 
public class SQLitePersistenceManager extends MetaPersistenceManager{

	String dbName;
	
	//@Override
	public DbConnectionManager getDbConnectionManager() {
		SQLiteDbManager dbManager; 
		if (null != this.dbName){
			dbManager = SQLiteDbManager.getInstance(dbName);
		} else {
			dbManager = SQLiteDbManager.getInstance();
		}
		return dbManager;
	}
	
	public SQLitePersistenceManager(){
		super();
	}
	
	public SQLitePersistenceManager(String dbName){
		this.dbName = dbName;
	}

	
}
