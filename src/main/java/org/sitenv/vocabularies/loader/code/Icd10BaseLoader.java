package org.sitenv.vocabularies.loader.code;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.log4j.Logger;
import org.sitenv.vocabularies.loader.VocabularyLoader;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by Brian on 2/7/2016.
 */
public abstract class Icd10BaseLoader extends IcdLoader implements VocabularyLoader {
	private static Logger logger = Logger.getLogger(Icd10BaseLoader.class);
    protected String oid;

	public long load(List<File> filesToLoad, DataSource datasource) {
		long n = 0;
		JdbcTemplate t = new JdbcTemplate(datasource);
		BufferedReader br = null;
		FileReader fileReader = null;
		try {
            String insertQueryPrefix = codeTableInsertSQLPrefix;
            StrBuilder insertQueryBuilder = new StrBuilder(insertQueryPrefix);
			int totalCount = 0, pendingCount = 0;

            for (File file : filesToLoad) {
                if (file.isFile() && !file.isHidden()) {
                    logger.debug("Loading ICD10CM File: " + file.getName());
                    fileReader = new FileReader(file);
                    br = new BufferedReader(fileReader);
                    String available;
                    while ((available = br.readLine()) != null) {
//                        if (pendingCount++ > 0) {
//                            insertQueryBuilder.append(",");
//                        }
//                        insertQueryBuilder.append("(");
//                        insertQueryBuilder.append("DEFAULT");
//                        insertQueryBuilder.append(",'");
//                        insertQueryBuilder.append(buildDelimitedIcdCode(available.substring(6, 13).trim()).toUpperCase());
//                        insertQueryBuilder.append("','");
//                        insertQueryBuilder.append(available.substring(77).trim().toUpperCase().replaceAll("'", "''"));
//                        insertQueryBuilder.append("','");
//                        insertQueryBuilder.append(file.getParentFile().getName());
//                        insertQueryBuilder.append("','");
//                        insertQueryBuilder.append(oid);
//                        insertQueryBuilder.append("')");

                    	n++;
                        t.update(insertQueryPrefix,buildDelimitedIcdCode(available.substring(6, 13).trim()).toUpperCase(),available.substring(77).trim().toUpperCase(),file.getParentFile().getName(),oid);
                    	
//                        if ((++totalCount % 2500) == 0) {
//                            doInsert(insertQueryBuilder.toString(), connection);
//                            insertQueryBuilder.clear();
//                            insertQueryBuilder.append(insertQueryPrefix);
//                            pendingCount = 0;
//                        }
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
