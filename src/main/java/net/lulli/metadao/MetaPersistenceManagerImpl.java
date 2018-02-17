package net.lulli.metadao;

import net.lulli.metadao.api.MetaDto;
import net.lulli.metadao.api.MetaPersistenceManager;
import net.lulli.metadao.api.WheresMap;
import net.lulli.metadao.impl.MetaDtoImpl;
import net.lulli.metadao.model.SQLDialect;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;


public abstract class MetaPersistenceManagerImpl implements MetaPersistenceManager
{
    static Logger log = Logger.getLogger("MetaPersistenceManagerImpl");
    private String tableName;

    String SQL_DIALECT = SQLDialect.STANDARD;

    public abstract DbConnectionManager getDbConnectionManager();

    //TODO return number of inserted records
    public Integer insert(MetaDto dto)
    {
        insertLegacy(dto);
        return new Integer(0);
    }

    private void insertLegacy(MetaDto dto)
    {
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDaoImpl dao;
        try
        {
            this.tableName = dto.getTableName();
            conn = dbManager.getConnection();
            log.trace("new MetaDaoImpl with tableName: [" + tableName + "]");
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

    //TODO return number of updated fields
    public Integer update(MetaDto dto, WheresMap wheres)
    {
        updateLegacy(dto, wheres);
        return new Integer(0);
    }

    public void updateLegacy(MetaDto dto, WheresMap wheres)
    {
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDaoImpl dao;
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

    //TODO return number of inserted records
    public Integer delete(MetaDto dto, WheresMap wheres)
    {
        deleteLegacy(dto, wheres);
        return new Integer(0);
    }

    private void deleteLegacy(MetaDto dto, WheresMap wheres)
    {
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDaoImpl dao;
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


    public Integer save(MetaDto dto, WheresMap wheres)
    {
        String retValue = saveLegacy(dto, (WheresMapImpl) wheres);
        return Integer.valueOf(retValue);
    }

    String saveLegacy(MetaDto dto, WheresMapImpl wheres)
    {
        //1 Verifica se il docVenditaDettaglio e' presente
        boolean isPresent = false;
        String tableName = dto.getTableName();
        String id = "";
        Connection conn = null;
        DbConnectionManager dbManager = getDbConnectionManager();
        MetaDaoImpl dao = null;
        try
        {
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            if (null != dao)
            {
                conn = dbManager.getConnection();
                Integer presenti = dao.selectCount(dto, wheres, conn, false);
                log.trace("TABLE:" + dto.getTableName());
                //isPresent = pm.isPresentByField(tableName, id_Azienda, codField, codValue);
                log.trace("[1]: CHECK PRESENCE");
                if (presenti > 0)
                {
                    removeNullKey(dto);
                    dao.update(dto, wheres, conn);
                } else
                {
                    //3 Se non presente inserisce
                    insert(dto);
                    log.trace("[3]: INSERT");
                }
            }
            //4 restituisce id inserito
        } catch (Exception e)
        {
            log.error("Insert/Update FALLITA" + e);
        }
        if (null != dao)
        {
            try
            {
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
        } else

        {
            return "0";

        }

    }

    private void removeNullKey(net.lulli.metadao.api.MetaDto dto)
    {
        if (dto.containsKey(null))
        {
            dto.remove(null);
            log.trace("METADAO_updateWhere: REMOVE_NULL");
        }
    }

    public Integer selectCount(MetaDto requestDto, WheresMap wheres, boolean definedAttributes)
    {
        //DbManager dbManager = DbManager.getInstance();
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDaoImpl dao;
        Integer count = 0;
        try
        {
            conn = dbManager.getConnection();
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            count = dao.selectCount(requestDto, (WheresMapImpl) wheres, conn, definedAttributes);
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
            net.lulli.metadao.api.MetaDto requestDto = MetaDtoImpl.of(tableName);
            Hashtable wheres = null;
            List<net.lulli.metadao.api.MetaDto> risultati = runQuery("select * from " + tableName);
            net.lulli.metadao.api.MetaDto rigaZero = risultati.get(0); //BAD: funziona solo se la tabella ha almeno 1 record!!
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
        MetaDaoImpl dao;
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
        MetaDaoImpl dao;
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

    public List<net.lulli.metadao.api.MetaDto> runQuery(String sqlInputString)
    {
        log.trace("BEGIN search");
        DbConnectionManager dbManager = getDbConnectionManager();
        Connection conn = null;
        MetaDaoImpl dao;
        List<net.lulli.metadao.api.MetaDto> results = null;

        String sqlDialect = this.SQL_DIALECT;
        try
        {
            log.trace("BEFORE conn");
            conn = dbManager.getConnection();
            log.trace("AFTER conn");
            dao = MetaDaoFactory.createMetaDao(SQL_DIALECT);
            if (null != sqlDialect)
            {
                dao.setDialect(sqlDialect);
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
                if (null != pstmt)
                {
                    pstmt.close();
                }
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
        return retvalue;
    }
}
