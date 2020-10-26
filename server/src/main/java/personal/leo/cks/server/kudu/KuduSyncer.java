package personal.leo.cks.server.kudu;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.config.props.CksProps;
import personal.leo.cks.server.exception.FatalException;
import personal.leo.cks.server.service.CksService;
import personal.leo.cks.server.util.IdUtils;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * TODO 目前是单线程的,后续考虑多线程
 */
@Slf4j
@Component
public class KuduSyncer {
    private final KuduClient kuduClient;
    private final CksService cksService;

    private CksProps.Kudu kuduProps;
    private KuduSession session;

    private final List<Operation> operations;
    private final Set<Long> batchIds = new HashSet<>();
    private final ConcurrentHashMap<String, KuduTable> kuduTableNameMapKuduTable = new ConcurrentHashMap<>();
    private final String[] datePatterns = {
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(),
            "yyyy-MM-dd HH:mm:ss"
    };

    private AtomicReference<SyncError> syncError = new AtomicReference<>();

    @Autowired
    public KuduSyncer(KuduClient kuduClient, CksService cksService, CksProps cksProps) {
        this.kuduClient = kuduClient;
        this.cksService = cksService;

        this.kuduProps = cksProps.getKudu();
        this.operations = new ArrayList<>(kuduProps.getMaxBatchSize());
    }


    @PostConstruct
    private void postConstruct() {
        session = kuduClient.newSession();

        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(kuduProps.getMaxBatchSize());

        final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.initialize();
        taskScheduler.scheduleAtFixedRate(() -> {
            try {
                sync();
            } catch (Exception e) {
                log.error("schedule kudu syncer error", e);
                syncError.set(new SyncError(batchIds, e));
            }
        }, Duration.ofMillis(kuduProps.getSyncPeriodMs()));
    }


    public synchronized void doOperation(CanalEntry.Entry entry, CanalEntry.RowChange rowChange, OperationType operationType, long batchId) throws KuduException, ParseException {
        final String srcTableId = IdUtils.buildSrcTableId(entry);
        final String kuduTableName = cksService.getKuduTableName(srcTableId);
        if (kuduTableName == null) {
            //TODO 跳过?
        } else {
            final KuduTable kuduTable = openKuduTable(kuduTableName);
            final Operation operation = createOperation(rowChange, kuduTableName, kuduTable, operationType);

            operations.add(operation);
            batchIds.add(batchId);

            //大于等于maxBatchSize的话,kudu apply的时候会报错
            if (operations.size() == kuduProps.getMaxBatchSize() - 1) {
                sync();
            }
        }
    }

    public SyncError getSyncError() {
        return syncError.get();
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
                throw new FatalException("not supported:" + operationType);
        }

        fillOperation(rowChange, kuduTableName, operationType, operation);

        return operation;
    }

    private void fillOperation(CanalEntry.RowChange rowChange, String kuduTableName, OperationType operationType, Operation operation) throws ParseException {
        final PartialRow row = operation.getRow();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            final List<CanalEntry.Column> srcModifiedColumns;
            final List<CanalEntry.Column> beforeColumns = rowData.getBeforeColumnsList();
            final List<CanalEntry.Column> afterColumns = rowData.getAfterColumnsList();
            switch (operationType) {
                case INSERT:
                    srcModifiedColumns = afterColumns;
                    break;
                case UPDATE:
                    srcModifiedColumns = findValueChangedColumns(beforeColumns, afterColumns, kuduTableName);
                    break;
                case DELETE:
                    srcModifiedColumns = beforeColumns;
                    break;
                default:
                    throw new FatalException("not supported:" + operationType);
            }

            for (CanalEntry.Column srcModifiedColumn : srcModifiedColumns) {
                final String kuduColumnName = srcModifiedColumn.getName();
                final String kuduColumnId = IdUtils.buildKuduColumnId(kuduTableName, kuduColumnName);
                final String srcColumnValue = srcModifiedColumn.getValue();
                final Type kuduColumnType = cksService.getKuduColumnType(kuduColumnId);
                if (kuduColumnType == null) {
                    //TODO 这种情况先忽略?
                } else {
                    fillRow(row, kuduColumnName, srcColumnValue, kuduColumnType);
                }
            }
        }
    }

    /**
     * TODO 如果kudu库中没有该条数据,会导致插入有误,因为会比较源库中的before和after,只会找到变更的值
     *
     * @param beforeColumns
     * @param afterColumns
     * @param kuduTableName
     * @return
     */
    private List<CanalEntry.Column> findValueChangedColumns(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns, String kuduTableName) {
        return afterColumns.stream()
                .map(afterColumn -> {
                    final String kuduColumnId = IdUtils.buildKuduColumnId(kuduTableName, afterColumn.getName());
                    final ColumnSchema kuduColumn = cksService.getKuduColumn(kuduColumnId);
                    if (kuduColumn == null) {
                        //TODO 咋整?先直接返回
                        return afterColumn;
                    } else {
                        if (kuduColumn.isKey()) {
                            return afterColumn;
                        } else if (!kuduColumn.isNullable()) {
                            return afterColumn;
                        } else {
//                            do nothing
                        }
                    }

                    final CanalEntry.Column beforeColumn = findColumn(beforeColumns, afterColumn);
                    if (beforeColumn == null) {
                        return afterColumn;
                    } else {
                        if (StringUtils.equals(beforeColumn.getValue(), afterColumn.getValue())) {
                            return null;
                        } else {
                            return afterColumn;
                        }
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 转map可能有报错风险,量不大的时候,用for循环获取反而高效安全
     *
     * @param columns
     * @param targetColumn
     * @return
     */
    private CanalEntry.Column findColumn(List<CanalEntry.Column> columns, CanalEntry.Column targetColumn) {
        if (CollectionUtils.isEmpty(columns) || targetColumn == null) {
            return null;
        }
        for (CanalEntry.Column column : columns) {
            if (StringUtils.equals(column.getName(), targetColumn.getName())) {
                return column;
            }
        }
        return null;
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
        if (StringUtils.isEmpty(srcColumnValue)) {
            if (kuduColumnType == Type.STRING) {
                row.addString(kuduColumnName, srcColumnValue);
            } else {
                row.addObject(kuduColumnName, null);
            }
            return;
        }
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
        final StopWatch watch = StopWatch.createStarted();
        for (Operation operation : operations) {
            session.apply(operation);
        }
        log.info("sync apply: " + operations.size());
        final List<OperationResponse> resps = session.flush();
        if (resps.size() > 0) {
            OperationResponse resp = resps.get(0);
            if (resp.hasRowError()) {
                //TODO 应该丢出怎样的异常?fatal?
                throw new RuntimeException("sync to kudu error:" + resp.getRowError());
            }
        }
        watch.stop();
        log.info("sync success: " + operations.size() + ", spend: " + watch);

        operations.clear();
        batchIds.clear();

        syncError.set(null);
    }

    @AllArgsConstructor
    @Getter
    public static class SyncError {
        private Set<Long> batchIds;
        private Exception exception;
    }

}
