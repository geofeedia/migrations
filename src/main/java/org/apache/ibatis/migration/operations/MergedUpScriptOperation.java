package org.apache.ibatis.migration.operations;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ibatis.migration.Change;
import org.apache.ibatis.migration.ConnectionProvider;
import org.apache.ibatis.migration.MigrationException;
import org.apache.ibatis.migration.MigrationLoader;
import org.apache.ibatis.migration.options.DatabaseOperationOption;

/**
 * @author szaboma
 */
public class MergedUpScriptOperation extends DatabaseOperation<MergedUpScriptOperation> {

    @Override
    public MergedUpScriptOperation operate(ConnectionProvider connectionProvider, MigrationLoader migrationsLoader, DatabaseOperationOption option, PrintStream printStream) {
        try {
            if (option == null) {
                option = new DatabaseOperationOption();
            }

            List<Change> pendingChanges = getPendingChanges(connectionProvider, migrationsLoader, option);

            for (Change change : pendingChanges) {
                printStream.println("-- " + change.getFilename());
                printChangeScript(printStream, migrationsLoader.getScriptReader(change, false));
                printStream.println();
                printStream.println();
                printStream.println(generateVersionInsert(change, option));
                printStream.println();
            }
            return this;
        } catch (Exception e) {
            throw new MigrationException("Error executing command.  Cause: " + e, e);
        }
    }

    private List<Change> getPendingChanges(ConnectionProvider connectionProvider, MigrationLoader migrationsLoader, DatabaseOperationOption option) {
        List<Change> pending = new ArrayList<Change>();
        List<Change> migrations = migrationsLoader.getMigrations();

        if (!changelogExists(connectionProvider, option)) {
            Collections.sort(migrations);
            return migrations;
        }

        List<Change> changelog = getChangelog(connectionProvider, option);
        for (Change change : migrations) {
            int index = changelog.indexOf(change);
            if (index < 0) {
                pending.add(change);
            }
        }
        Collections.sort(pending);
        return pending;
    }

    private String generateVersionInsert(Change change, DatabaseOperationOption option) {
        return "INSERT INTO " + option.getChangelogTable() + " (ID, APPLIED_AT, DESCRIPTION) " +
                "VALUES (" + change.getId() + ", '" + DatabaseOperation.generateAppliedTimeStampAsString() + "', '"
                + change.getDescription().replace('\'', ' ') + "')" + option.getDelimiter();
    }

    private void printChangeScript(PrintStream printStream, Reader migrationReader) throws IOException {
        char[] cbuf = new char[1024];
        int l;
        while ((l = migrationReader.read(cbuf)) == cbuf.length) {
            printStream.print(new String(cbuf, 0, l));
        }
        if (l > 0) {
            printStream.print(new String(cbuf, 0, l - 1));
        }
    }
}
