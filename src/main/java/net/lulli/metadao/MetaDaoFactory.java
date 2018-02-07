package net.lulli.metadao;


import java.util.Hashtable;

public class MetaDaoFactory
{

    private static Hashtable<String, Object> daoPool = new Hashtable<String, Object>();
    private static MetaDao metaDao;

    public static MetaDao createMetaDao(String sqlDialect)
    {
        return getNewMetaDao();

    }

    public static MetaDao getNewMetaDao()
    {
        if (null == metaDao)
        {
            metaDao = new MetaDao();
        }
        return metaDao;
    }

}
