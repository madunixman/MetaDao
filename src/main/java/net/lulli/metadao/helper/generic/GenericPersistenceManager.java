package net.lulli.metadao.helper.generic;

import java.util.Properties;

import net.lulli.metadao.DbConnectionManager;
import net.lulli.metadao.DbManager;
import net.lulli.metadao.model.MetaPersistenceManager;
import net.lulli.utils.PropertiesManager;

 
public class GenericPersistenceManager extends MetaPersistenceManager{
	
public DbConnectionManager getDbConnectionManager() {
		
		// SE NON VUOI RIDEFINIRLO::: 
		//DbManager dbManager =  DbManager.getInstance();
		//this.setSQLDialect(MYSQL);
		 
		GenericDbManager dbManager     = GenericDbManager.getInstance();
		PropertiesManager    configManager = PropertiesManager.getInstance();
		Properties           dbProperties  = configManager.getProperties();
		return dbManager;
	}	
	
}
