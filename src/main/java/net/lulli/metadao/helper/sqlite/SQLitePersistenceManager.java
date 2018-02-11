package net.lulli.metadao.helper.sqlite;

import net.lulli.metadao.DbConnectionManager;
import net.lulli.metadao.MetaPersistenceManagerImpl;

public class SQLitePersistenceManager  extends MetaPersistenceManagerImpl
{
    String dbName;

    public DbConnectionManager getDbConnectionManager()
    {
        SQLiteDbManager dbManager;
        if (null != this.dbName)
        {
            dbManager = SQLiteDbManager.getInstance(dbName);
        } else
        {
            dbManager = SQLiteDbManager.getInstance();
        }
        return dbManager;
    }

    public SQLitePersistenceManager()
    {
        super();
    }

    public SQLitePersistenceManager(String dbName)
    {
        this.dbName = dbName;
    }

    //@Deprecated
    //public Integer selectCount(MetaDtoImpl var1, Map wheres, boolean var3){
    //       return new Integer(0);
    //}
}
