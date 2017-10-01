package org.immunetolerance.biocat.querycat;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.labkey.remoteapi.CommandException;
import org.labkey.remoteapi.Connection;
import org.labkey.remoteapi.query.ExecuteSqlCommand;
import org.labkey.remoteapi.query.SelectRowsCommand;
import org.labkey.remoteapi.query.SelectRowsResponse;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processes user input to run LabKey sql query on target Labkey structure and write results to database tables.
 */
class QueryRunner {
    private final String serverAddress;
    private final String username;
    private final String password;
    private final String labkeyPath;
    private final String labkeySchema;
    private final String labkeyQuery;
    private final String jdbcString;
    private final String option;
    private String study;

    QueryRunner(String serverAddress, String username, String password, String labkeyPath, String labkeySchema, String labkeyQuery, String jdbcString, String option) {
        this.serverAddress = serverAddress;
        this.username = username;
        this.password = password;
        this.labkeyPath = labkeyPath;
        this.jdbcString = jdbcString;
        this.labkeySchema = labkeySchema;
        this.labkeyQuery = labkeyQuery;
        this.option = option;
        this.study = null;
    }


    /**
     * Main program runner.
     */
    void run() {
        this.createSchema();

        if (option != null && option.equalsIgnoreCase("all")) {
            for (String lkQuery : this.createQueryList()) {
                this.executeSQL(lkQuery);
            }
        } else {
            this.executeSQL(labkeyQuery);
        }
    }


    /**
     * Creates a new database schema, if it did not already exist, corresponding to the input LabKey schema.
     * If the LabKey schema is 'study', the db schema created will be given by the study name ('ITNXXXYY') specified in
     * the input LabKey path.
     */
    private void createSchema() {
        try {
            SqlWriter sw = SqlWriter.getInstance();
            sw.getConnection(jdbcString);

            if (labkeySchema.equalsIgnoreCase("study")) {
                //todo: this is trialshare specific. make generic.
                Matcher m = Pattern.compile("^/Studies/(ITN\\d{3}[a-zA-Z]{2})OPR/.+$", Pattern.CASE_INSENSITIVE).matcher(labkeyPath);
                if (m.matches()) {
                    study = m.group(1);
                } else {
                    System.out.println("Incorrect path formatting for 'study' schema.");
                    System.exit(0);
                }

                sw.createSchema(study);
            } else {
                sw.createSchema(labkeySchema);
            }

            sw.closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * Creates a list of LabKey queries based on the input LabKey SQL query. Finds all datasets for a given study and
     * generates a corresponding LabKey SQL statement to query it.
     *
     * @return      list of LabKey sql queries, one for each dataset in the study
     */
    private List<String> createQueryList() {
        Matcher m = Pattern.compile("^SELECT .+ FROM (\\w+)$").matcher(labkeyQuery);
        List<String> queryList = new ArrayList<>();

        if (!m.matches()) {
            System.out.println("Labkey sql input is incompatible with option \"all\".");
            System.exit(0);
        }

        String target = m.group(1);

        Connection cn = new Connection(serverAddress, username, password);
        SelectRowsCommand cmd = new SelectRowsCommand(labkeySchema, "DataSets");
        cmd.setRequiredVersion(9.1);
        cmd.setColumns(Collections.singletonList("Name"));

        try {
            SelectRowsResponse response = cmd.execute(cn, labkeyPath);
            for (Map<String, Object> row : response.getRows()) {
                String dataset = ((JSONObject) row.get("Name")).get("value").toString();
                queryList.add(labkeyQuery.replace(target, dataset));
            }
        } catch (IOException | CommandException e) {
            e.printStackTrace();
        }

        return queryList;
    }


    /**
     * Executes a LabKey SQL query and writes the result to a database table.
     * @param   lkQuery     a 'select' LabKey SQL query
     */
    private void executeSQL(String lkQuery) {
        Matcher m = Pattern.compile("^SELECT .+ FROM (\\w+).?+$").matcher(lkQuery);
        if (!m.matches()) {
            System.out.println("Cannot find target table in sql: " + lkQuery);
        }
        String targetTable = m.group(1);

        ExecuteSqlCommand cmd = new ExecuteSqlCommand(labkeySchema, lkQuery);
        cmd.setRequiredVersion(9.1);
        SelectRowsResponse response;

        try {
            Connection cn = new Connection(serverAddress, username, password);
            response = cmd.execute(cn, labkeyPath);
            System.out.println("Number of rows: " + response.getRowCount());

            Map<String, Object> cm = response.getMetaData();
            JSONArray fieldArray = (JSONArray) cm.get("fields");
            Map<String, DbCol> cols = new HashMap<>();

            for (Object o : fieldArray) {
                JSONObject cjo = (JSONObject) o;
                DbCol dc = new DbCol(cjo);
                String fieldName = (String) ((JSONArray) cjo.get("fieldKeyArray")).get(0);
                cols.put(fieldName, dc);
            }

            List<Map<String, Object>> rows = response.getRows();
            Set<Set<DbField>> results = new HashSet<>();
            if (rows.size() > 0) {
                for (Map<String, Object> row : rows) {
                    Set<DbField> thisRow = new HashSet<>();
                    for (String key : row.keySet()) {

                        switch (cols.get(key).type) {
                            case "date":
                                if (row.get(key) != null) {
                                    JSONObject jfield = (JSONObject) (row.get(key));
                                    if (jfield != null && jfield.get("value") != null) {
                                        String dateString = String.valueOf(((JSONObject) row.get(key)).get("value"));
                                        DateTimeFormatter f = DateTimeFormatter.ofPattern("EE MMM dd HH:mm:ss z uuuu");
                                        ZonedDateTime zdt = ZonedDateTime.parse(dateString, f);
                                        Instant instant = zdt.toInstant();
                                        thisRow.add(new DbField(key, instant));
                                    } else {
                                        thisRow.add(new DbField(key, null));
                                    }
                                } else {
                                    thisRow.add(new DbField(key, null));
                                }
                                break;
                            default:
                                thisRow.add(new DbField(key, ((JSONObject) row.get(key)).get("value")));
                        }
                    }
                    results.add(thisRow);
                }
            }

            try {
                SqlWriter sw = SqlWriter.getInstance();
                sw.getConnection(jdbcString);

                if (study == null) {
                    sw.createTable(targetTable, cols, labkeySchema);
                    sw.update(cols, results, targetTable, labkeySchema);
                } else {
                    sw.createTable(targetTable, cols, study);
                    sw.update(cols, results, targetTable, study);
                }
                sw.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (IOException | CommandException e) {
            e.printStackTrace();
        }
    }
}
