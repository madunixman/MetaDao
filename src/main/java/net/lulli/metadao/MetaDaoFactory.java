package net.lulli.metadao;


import java.util.Hashtable;

public class MetaDaoFactory
{

    private static Hashtable<String, Object> daoPool = new Hashtable<String, Object>();
    private static MetaDaoImpl metaDao;

    public static MetaDaoImpl createMetaDao(String sqlDialect)
    {
        return getNewMetaDao();

    }

    public static MetaDaoImpl getNewMetaDao()
    {
        if (null == metaDao)
        {
            metaDao = new MetaDaoImpl();
        }
        return metaDao;
    }

}
