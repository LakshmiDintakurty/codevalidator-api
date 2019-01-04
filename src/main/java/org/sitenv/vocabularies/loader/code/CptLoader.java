package org.sitenv.vocabularies.loader.code;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.log4j.Logger;
import org.sitenv.vocabularies.loader.BaseVocabularyLoader;
import org.sitenv.vocabularies.loader.VocabularyLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

/**
 * Created by Brian on 2/7/2016.
 */
@Component(value = "CPT")
public class CptLoader extends BaseVocabularyLoader implements VocabularyLoader {
    private static Logger logger = Logger.getLogger(CptLoader.class);
    private String oid;

    public CptLoader() {
        this.oid = CodeSystemOIDs.CPT4.codesystemOID();
    }

    public long load(List<File> filesToLoad, DataSource ds) {
    	long n = 0;
    	JdbcTemplate t = new JdbcTemplate(ds);
        BufferedReader br = null;
        FileReader fileReader = null;
        String insertQueryPrefix = codeTableInsertSQLPrefix;
        try {
//            StrBuilder insertQueryBuilder = new StrBuilder(codeTableInsertSQLPrefix);
//            int totalCount = 0, pendingCount = 0;

            for (File file : filesToLoad) {
                if (file.isFile() && !file.isHidden()) {
                    logger.debug("Loading CPT File: " + file.getName());
                    String codeSystem = file.getParentFile().getName();
                    fileReader = new FileReader(file);
                    br = new BufferedReader(fileReader);
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (!line.isEmpty()) {
                            String code = line.substring(0, 5);
                            String displayName = line.substring(line.indexOf(" "));
                                                        
                            
//                            buildCodeInsertQueryString(insertQueryBuilder, code, displayName, codeSystem, oid, CODES_IN_THIS_SYSTEM_ARE_ALWAYS_ACTIVE);
//                            if ((++totalCount % BATCH_SIZE) == 0) {
//                                insertCode(insertQueryBuilder.toString(), connection);
//                                insertQueryBuilder.clear();
//                                insertQueryBuilder.append(codeTableInsertSQLPrefix);
//                                pendingCount = 0;
//                            }
                            
                            //"insert into CODES (CODE, DISPLAYNAME, CODESYSTEM, CODESYSTEMOID) values (?,?,?,?)";
                        	n++;
                        	code = trimAndChangeToUpperCase(code);
                        	displayName = trimAndChangeToUpperCase(displayName);
                        	codeSystem = trimAndChangeToUpperCase(codeSystem);
                            t.update(insertQueryPrefix, code, displayName, codeSystem, oid);
                            logger.trace("Inserted CPT Code : " + code + " and displayName : " + displayName);
                        }
                    }
                }
            }
//            if (pendingCount > 0) {
//                insertCode(insertQueryBuilder.toString(), connection);
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
        logger.debug("Number of CPT Codes processed : " + n);
        return n;
    }

	private String trimAndChangeToUpperCase(String value) {
		return value = (StringUtils.isBlank(value)) ? value : value.trim().toUpperCase();
	}
}