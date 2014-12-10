package org.apache.ibatis.migration.commands;

import java.math.BigDecimal;
import java.util.Properties;
import org.apache.ibatis.migration.Change;
import org.apache.ibatis.migration.operations.MergedUpScriptOperation;
import org.apache.ibatis.migration.options.SelectedOptions;

/**
 * @author szaboma
 */
public class MergedUpScriptCommand extends BaseCommand {

    public MergedUpScriptCommand(SelectedOptions options) {
        super(options);
    }

    public void execute(String... sparams) {
        MergedUpScriptOperation operation = new MergedUpScriptOperation();
        operation.operate(getConnectionProvider(), getMigrationLoader(), getDatabaseOperationOption(), printStream);

    }


    private boolean shouldRun(Change change, BigDecimal v1, BigDecimal v2) {
        BigDecimal id = change.getId();
        if (v1.compareTo(v2) > 0) {
            return (id.compareTo(v2) > 0 && id.compareTo(v1) <= 0);
        } else {
            return (id.compareTo(v1) > 0 && id.compareTo(v2) <= 0);
        }
    }

    // Issue 699
    private String getDelimiter() {
        Properties props = environmentProperties();
        StringBuilder delimiter = new StringBuilder();
        if (Boolean.valueOf(props.getProperty("full_line_delimiter"))) delimiter.append("\n");
        delimiter.append(props.getProperty("delimiter", ";"));
        return delimiter.toString();
    }
}
