package net.lulli.metadao;

import net.lulli.metadao.extension.cassandra.CassandraMetaDao;
import net.lulli.metadao.model.SQLDialect;

public class MetaDaoFactory {
	
	public static MetaDao createMetaDao(String sqlDialect)
	{
		if (SQLDialect.CASSANDRA.equals(sqlDialect))
		{
			return new CassandraMetaDao();
		}
		if (SQLDialect.STANDARD.equals(sqlDialect))
		{
			return new MetaDao();
		}
		else
		{
			return new MetaDao();
		}

	}
	
}
