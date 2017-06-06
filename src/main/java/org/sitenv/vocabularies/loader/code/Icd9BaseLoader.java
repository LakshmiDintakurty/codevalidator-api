package org.sitenv.vocabularies.loader.code;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.log4j.Logger;
import org.sitenv.vocabularies.loader.BaseCodeLoader;
import org.sitenv.vocabularies.validation.dao.CodeSystemCodeDAO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sitenv.vocabularies.validation.dao.CodeSystemCodeDAO;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import static org.sitenv.vocabularies.loader.code.IcdLoader.buildDelimitedIcdCode;

/**
 * Created by Brian on 2/7/2016.
 */
public abstract class Icd9BaseLoader extends BaseCodeLoader {
    private static Logger logger = Logger.getLogger(Icd9BaseLoader.class);
    protected String oid;

    @Override
    public long load(List<File> filesToLoad, DataSource datasource) {
    	long n = 0;
        BufferedReader br = null;
        FileReader fileReader = null;
        JdbcTemplate t = new JdbcTemplate(datasource);
        try {
            StrBuilder insertQueryBuilder = new StrBuilder(codeTableInsertSQLPrefix);
            int totalCount = 0, pendingCount = 0;

            for (File file : filesToLoad) {
                if (file.isFile() && !file.isHidden()) {
                    logger.debug("Loading ICD9 File: " + file.getName());
                    String codeSystem = file.getParentFile().getName();
                    fileReader = new FileReader(file);
                    br = new BufferedReader(fileReader);
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (!line.isEmpty()) {
                            String code = buildDelimitedIcdCode(line.substring(0, 5));
                            String displayName = line.substring(6);
//                            buildCodeInsertQueryString(insertQueryBuilder, code, displayName, codeSystem, oid);
//                            if (pendingCount++ > 0) {
//                                insertQueryBuilder.append(",");
//                            }
//                            insertQueryBuilder.append("(");
//                            insertQueryBuilder.append("DEFAULT");
//                            insertQueryBuilder.append("'");
//                            insertQueryBuilder.append(buildDelimitedIcdCode(line.substring(0, 5).trim()).toUpperCase());
//                            insertQueryBuilder.append("','");
//                            insertQueryBuilder.append(line.substring(6).trim().toUpperCase().replaceAll("'", "''"));
//                            insertQueryBuilder.append("','");
//                            insertQueryBuilder.append(file.getParentFile().getName());
//                            insertQueryBuilder.append("','");
//                            insertQueryBuilder.append(oid);
//                            insertQueryBuilder.append("')");
                  
                        	n++;
                            t.update(codeTableInsertSQLPrefix,code.trim().toUpperCase(),displayName.trim().toUpperCase(),codeSystem,oid);
                            
//                            if ((++totalCount % 5000) == 0) {
//                                doInsert(insertQueryBuilder.toString(), connection);
//                                insertQueryBuilder.clear();
//                                insertQueryBuilder.append(insertQueryPrefix);
//                                pendingCount = 0;
//                            }
                        }
                    }
                }
            }
//            if (pendingCount > 0) {
//                doInsert(insertQueryBuilder.toString(), connection);
//            }
        } catch (IOException e) {
            logger.error(e);
//        } catch (SQLException e) {
//            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    fileReader.close();
                    br.close();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }
        return n;
    }

}
