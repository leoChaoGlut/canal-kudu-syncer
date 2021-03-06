package personal.leo.cks.server.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduTable;

public class IdUtils {

    public static final String DOT = ".";

    /**
     * TODO 最好带上instanceid
     *
     * @param entry
     * @return
     */
    public static String buildSrcTableId(CanalEntry.Entry entry) {
        return entry.getHeader().getSchemaName() + DOT + entry.getHeader().getTableName();
    }

    public static String buildSrcTableId(String srcSchemaName, String srcTableName) {
        return srcSchemaName + DOT + srcTableName;
    }

    public static String buildKuduColumnId(String kuduTableName, String kuduColumnName) {
        return kuduTableName + DOT + kuduColumnName;
    }

    public static String buildKuduColumnId(KuduTable kuduTable, ColumnSchema kuduColumn) {
        return buildKuduColumnId(kuduTable.getName(), kuduColumn.getName());
    }
}
