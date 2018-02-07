package net.lulli.metadao;

import net.lulli.metadao.api.IMetaPersistenceManager;
import net.lulli.metadao.api.MetaDto;
import net.lulli.metadao.model.SQLDialect;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;


public abstract class MetaPersistenceManager implements IMetaPersistenceManager
{
    static Logger log = Logger.getLogger("MetaPersistenceManager");
    private String tableName;

    String SQL_DIALECT = SQLDialect.STANDARD;

    public abstract DbConnectionManager getDbConnectionManager();

    public void insert(MetaDto dto)
    {
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDao dao;
        try
        {
            this.tableName = dto.getTableName();
            conn = dbManager.getConnection();
            log.trace("new MetaDao with tableName: [" + tableName + "]");
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            dao.insert(dto, conn);
        } catch (Exception e)
        {
            log.trace(e.getMessage());
        } finally
        {
            dbManager.releaseConnection(conn);
        }
    }

    public void update(MetaDto dto, Hashtable wheres)
    {
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDao dao;
        try
        {
            this.tableName = dto.getTableName();
            conn = dbManager.getConnection();
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            dao.update(dto, wheres, conn);
        } catch (Exception e)
        {
            log.trace(e.getMessage());
        } finally
        {
            dbManager.releaseConnection(conn);
        }
    }

    public void delete(MetaDto dto, Hashtable wheres)
    {
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDao dao;
        try
        {
            this.tableName = dto.getTableName();
            conn = dbManager.getConnection();
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            dao.delete(dto, wheres, conn);
        } catch (Exception e)
        {
            log.trace(e.getMessage());
        } finally
        {
            dbManager.releaseConnection(conn);
        }
    }


    public String save(MetaDto dto, Hashtable wheres)
    {
        //1 Verifica se il docVenditaDettaglio e' presente
        boolean isPresent = false;
        String tableName = dto.getTableName();
        String id = "";
        Connection conn = null;
        DbConnectionManager dbManager = getDbConnectionManager();
        MetaDao dao = null;
        try
        {
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            conn = dbManager.getConnection();
            String presenti = dao.selectCount(dto, wheres, conn, false);
            log.trace("TABLE:" + dto.getTableName());
            //isPresent = pm.isPresentByField(tableName, id_Azienda, codField, codValue);
            log.trace("[1]: CHECK PRESENCE");
            if (Integer.valueOf(presenti) > 0)
            {
                removeNullKey(dto);
                dao.update(dto, wheres, conn);
            } else
            {
                //3 Se non presente inserisce
                insert(dto);
                log.trace("[3]: INSERT");
            }
            //4 restituisce id inserito
        } catch (Exception e)
        {
            log.error("Insert/Update FALLITA" + e);
        }
        try
        {
            //pm.select
            id = dao.selectIdWhere(dto, wheres, conn, false, 1);
        } catch (Exception e)
        {
            log.error("Cannot decode record from table=[" + tableName + "]");
        } finally
        {
            dbManager.releaseConnection(conn);
        }
        log.trace("[4]: RETURN_ID");
        return id;
    }

    private void removeNullKey(MetaDto dto)
    {
        if (dto.containsKey(null))
        {
            dto.remove(null);
            log.trace("METADAO_updateWhere: REMOVE_NULL");
        }
    }

    public Integer selectCount(MetaDto requestDto, Hashtable wheres, boolean definedAttributes)
    {
        //DbManager dbManager = DbManager.getInstance();
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDao dao;
        Integer count = 0;
        try
        {
            conn = dbManager.getConnection();
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            count = Integer.valueOf(dao.selectCount(requestDto, wheres, conn, definedAttributes));
        } catch (Exception e)
        {
            log.error("" + e);
        }
        return count;
    }

    public List<String> descTable(String tableName)
    {
        List<String> fieldList = null;
        String sqlDialect = this.getSQLDialect();
        log.trace("Detected SQLDialect:[" + sqlDialect + "]");

        if (sqlDialect.equals(STANDARD))
        {
            fieldList = descTableConcrete(tableName);
        }
        return fieldList;
    }

    private List<String> descTableConcrete(String tableName)
    {
        List<String> fieldList = new ArrayList<String>();
        try
        {
            MetaDto requestDto = MetaDto.getNewInstance(tableName);
            Hashtable wheres = null;
            List<MetaDto> risultati = runQuery("select * from " + tableName);
            MetaDto rigaZero = risultati.get(0); //BAD: funziona solo se la tabella ha almeno 1 record!!
            Set<String> keys = rigaZero.keySet();
            Iterator<String> iter = keys.iterator();
            String field;
            while (iter.hasNext())
            {
                field = iter.next();
                fieldList.add(field);
            }
        } catch (Exception e)
        {
            log.error("" + e);
        }
        return fieldList;
    }

    public boolean createTable(String tableName, List<String> fields)
    {
        boolean created = false;
        DbConnectionManager dbManager;
        MetaDao dao;
        Connection conn;
        try
        {
            dbManager = getDbConnectionManager();
            conn = dbManager.getConnection();
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            created = dao.createTable(tableName, fields, conn);
        } catch (Exception e)
        {
            log.error("" + e);
        }
        return created;
    }

    public boolean dropTable(String tableName)
    {
        boolean dropped = false;
        DbConnectionManager dbManager;
        MetaDao dao;
        Connection conn;
        try
        {
            dbManager = getDbConnectionManager();
            conn = dbManager.getConnection();
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            dropped = dao.dropTable(tableName, conn);
        } catch (Exception e)
        {
            log.error("" + e);
        }
        return dropped;
    }

    public String getSQLDialect()
    {
        return SQL_DIALECT;
    }

    public void setSQLDialect(String sqlDialect)
    {
        SQL_DIALECT = sqlDialect;
    }

    public List<MetaDto> runQuery(String sqlInputString)
    {
        log.trace("BEGIN search");
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDao dao;
        List<MetaDto> results = null;

        String sqlDialect = this.SQL_DIALECT;
        try
        {
            log.trace("BEFORE conn");
            conn = dbManager.getConnection();
            log.trace("AFTER conn");
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            if (null != sqlDialect)
            {
                dao.setSqlDialect(sqlDialect);
            }
            results = dao.runQuery(sqlInputString, conn);
        } catch (Exception e)
        {
            log.trace(e.getMessage());
        } finally
        {
            dbManager.releaseConnection(conn);
        }
        return results;
    }

    public int execute(String sqlInputString)
    {
        int retvalue = 0;
        PreparedStatement pstmt = null;
        Connection conn = null;
        DbConnectionManager dbManager = getDbConnectionManager();
        try
        {
            conn = dbManager.getConnection();
            log.debug("sqlInputString = [" + sqlInputString + "]");
            pstmt = conn.prepareStatement(sqlInputString);
            retvalue = pstmt.executeUpdate();
        } catch (Exception e)
        {
            log.error(e.getMessage());
        } finally
        {
            try
            {
                pstmt.close();
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
        return retvalue;
    }
}
