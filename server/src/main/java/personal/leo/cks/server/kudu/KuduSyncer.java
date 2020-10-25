package personal.leo.cks.server.kudu;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.util.IdUtils;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO 目前是单线程的,后续考虑多线程
 */
@Slf4j
@Component
public class KuduSyncer {
    @Autowired
    private KuduClient kuduClient;
    @Autowired
    private Map<String, String> srcTableIdMapKuduTableName;
    @Autowired
    private Map<String, Type> kuduColumnIdMapKuduColumnType;

    private KuduSession session;

    /**
     * 不可大于 org.apache.kudu.client.AsyncKuduSession#mutationBufferMaxOps,否则kuduclient会报错
     *
     * @see AsyncKuduSession#apply(org.apache.kudu.client.Operation)
     * @see org.apache.kudu.client.AsyncKuduSession#mutationBufferMaxOps
     */
    private final int maxBatchSize = 1000;
    private final int syncDurationMs = 1000;
    private final List<Operation> operations = new ArrayList<>(maxBatchSize);
    private final ConcurrentHashMap<String, KuduTable> kuduTableNameMapKuduTable = new ConcurrentHashMap<>();
    private final String[] datePatterns = {
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(),
            "yyyy-MM-dd HH:mm:ss"
    };

    @PostConstruct
    private void postConstruct() {
        session = kuduClient.newSession();

        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(maxBatchSize);

        new ThreadPoolTaskScheduler().scheduleAtFixedRate(() -> {
            try {
                sync();
            } catch (Exception e) {
                log.error("schedule kudu syncer error", e);
            }
        }, Duration.ofMillis(syncDurationMs));
    }


    public synchronized void doOperation(CanalEntry.Entry entry, CanalEntry.RowChange rowChange, OperationType operationType) throws KuduException, ParseException {
        final String srcTableId = IdUtils.buildSrcTableId(entry);
        final String kuduTableName = srcTableIdMapKuduTableName.get(srcTableId);
        if (kuduTableName == null) {
            //TODO 跳过?
        } else {
            final KuduTable kuduTable = openKuduTable(kuduTableName);
            final Operation operation = createOperation(rowChange, kuduTableName, kuduTable, operationType);

            operations.add(operation);

            //大于等于maxBatchSize的话,kudu apply的时候会报错
            if (operations.size() == maxBatchSize - 1) {
                sync();
            }
        }
    }

    private Operation createOperation(CanalEntry.RowChange rowChange, String kuduTableName, KuduTable kuduTable, OperationType operationType) throws ParseException {
        final Operation operation;
        switch (operationType) {
            case INSERT:
            case UPDATE:
                operation = kuduTable.newUpsert();
                break;
            case DELETE:
                operation = kuduTable.newDelete();
                break;
            default:
                throw new RuntimeException("not supported:" + operationType);
        }

        fillOperation(rowChange, kuduTableName, operationType, operation);

        return operation;
    }

    private void fillOperation(CanalEntry.RowChange rowChange, String kuduTableName, OperationType operationType, Operation operation) throws ParseException {
        final PartialRow row = operation.getRow();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            final List<CanalEntry.Column> srcColumns;
            switch (operationType) {
                case INSERT:
                case UPDATE:
                    srcColumns = rowData.getAfterColumnsList();
                    break;
                case DELETE:
                    srcColumns = rowData.getBeforeColumnsList();
                    break;
                default:
                    throw new RuntimeException("not supported:" + operationType);
            }

            for (CanalEntry.Column srcColumn : srcColumns) {
                final String kuduColumnName = srcColumn.getName();
                final String kuduColumnId = IdUtils.buildKuduColumnId(kuduTableName, kuduColumnName);
                final String srcColumnValue = srcColumn.getValue();
                final Type kuduColumnType = kuduColumnIdMapKuduColumnType.get(kuduColumnId);
                if (kuduColumnType == null) {
                    //TODO 这种情况先忽略?
                } else {
                    fillRow(row, kuduColumnName, srcColumnValue, kuduColumnType);
                }
            }
        }
    }

    /**
     * copy from org.apache.kudu.client.PartialRow.addObject(int, java.lang.Object)
     *
     * @param row
     * @param kuduColumnName
     * @param srcColumnValue
     * @param kuduColumnType
     * @throws ParseException
     */
    private void fillRow(PartialRow row, String kuduColumnName, String srcColumnValue, Type kuduColumnType) throws ParseException {
        switch (kuduColumnType) {
            case BOOL:
                row.addBoolean(kuduColumnName, Boolean.parseBoolean(srcColumnValue));
                break;
            case INT8:
                row.addByte(kuduColumnName, Byte.parseByte(srcColumnValue));
                break;
            case INT16:
                row.addShort(kuduColumnName, Short.parseShort(srcColumnValue));
                break;
            case INT32:
                row.addInt(kuduColumnName, Integer.parseInt(srcColumnValue));
                break;
            case INT64:
                row.addLong(kuduColumnName, Long.parseLong(srcColumnValue));
                break;
            case UNIXTIME_MICROS:
                final Date date = DateUtils.parseDate(srcColumnValue, datePatterns);
                row.addTimestamp(kuduColumnName, new Timestamp(date.getTime()));
                break;
            case FLOAT:
                row.addFloat(kuduColumnName, Float.parseFloat(srcColumnValue));
                break;
            case DOUBLE:
                row.addDouble(kuduColumnName, Double.parseDouble(srcColumnValue));
                break;
            case STRING:
                row.addString(kuduColumnName, srcColumnValue);
                break;
            case BINARY:
                row.addBinary(kuduColumnName, srcColumnValue.getBytes(StandardCharsets.UTF_8));
                break;
            case DECIMAL:
                row.addDecimal(kuduColumnName, new BigDecimal(srcColumnValue));
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + kuduColumnType);
        }
    }

    private KuduTable openKuduTable(String kuduTableName) throws KuduException {
        KuduTable kuduTable = kuduTableNameMapKuduTable.get(kuduTableName);
        if (kuduTable == null) {
            kuduTable = kuduClient.openTable(kuduTableName);
            kuduTableNameMapKuduTable.put(kuduTableName, kuduTable);
        }
        return kuduTable;
    }


    private synchronized void sync() throws KuduException {
        if (operations.isEmpty()) {
            return;
        }

        for (Operation operation : operations) {
            session.apply(operation);
        }

        final List<OperationResponse> resps = session.flush();
        if (resps.size() > 0) {
            OperationResponse resp = resps.get(0);
            if (resp.hasRowError()) {
                throw new RuntimeException("sync to kudu error:" + resp.getRowError());
            }
        }

        operations.clear();
    }


}
