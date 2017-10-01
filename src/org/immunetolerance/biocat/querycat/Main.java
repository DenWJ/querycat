package org.immunetolerance.biocat.querycat;

public class Main {

    public static void main(String[] args) {
        String serverAddress;
        String username;
        String password;
        String labkeyPath;
        String labkeySchema;
        String labkeyQuery;
        String jdbcString;
        String option = null;

        if (args.length >= 7) {
            serverAddress = args[0];
            username = args[1];
            password = args[2];
            labkeyPath = args[3];
            labkeySchema = args[4];
            labkeyQuery = args[5];
            jdbcString = args[6];

            if (args.length >= 8) {
                option = args[7]; // all
            }

            QueryRunner sm = new QueryRunner(serverAddress, username, password, labkeyPath, labkeySchema, labkeyQuery, jdbcString, option);
            sm.run();
        } else {
            System.out.println("See readme for example usage.");
        }
    }
}