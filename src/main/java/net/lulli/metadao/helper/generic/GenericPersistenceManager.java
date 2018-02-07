package net.lulli.metadao.helper.generic;

import java.util.Properties;

import net.lulli.metadao.DbConnectionManager;
import net.lulli.metadao.DbManager;
import net.lulli.metadao.MetaPersistenceManager;
import net.lulli.utils.PropertiesManager;

 
public class GenericPersistenceManager extends MetaPersistenceManager{
	
	/**
	 * getter for {@link DbManager} class
	 *  If you don't want to redefine it
	 *   DbManager dbManager =  DbManager.getInstance();
	 *   this.setSQLDialect(MYSQL);
	 */ 
	public DbConnectionManager getDbConnectionManager() {
		GenericDbManager dbManager     = GenericDbManager.getInstance();
		PropertiesManager    configManager = PropertiesManager.getInstance();
		Properties           dbProperties  = configManager.getProperties();
		return dbManager;
	}	
	
}
