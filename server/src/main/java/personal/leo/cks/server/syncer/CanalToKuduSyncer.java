package personal.leo.cks.server.syncer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.config.props.CksProps;
import personal.leo.cks.server.zk.ZkService;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CanalToKuduSyncer {

    @Autowired
    ZkService zkService;
    @Autowired
    CanalConnector canalConnector;
    @Autowired
    CksProps cksProps;
    @Autowired
    KuduClient kuduClient;
    @Autowired
    Map<String, Type> columnNameMapType;
    @Autowired
    TaskExecutor cksTaskExecutor;

    @PostConstruct
    private void postConstruct() {
        consume();
    }

    /**
     * TODO 失败告警邮件
     */
    public void consume() {
        while (true) {
            if (zkService.isMaster()) {
                doConsume();
            } else {
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    log.error("sleep error", e);
                }
            }
        }
    }

    private void doConsume() {
        try {
            canalConnector.connect();
            canalConnector.subscribe();

            int maxRetryTimes = cksProps.getCanal().getMaxRetryTimes() >= 0 ? cksProps.getCanal().getMaxRetryTimes() : Integer.MAX_VALUE;

            while (zkService.isMaster()) {
                for (int retryTimes = 0; retryTimes <= maxRetryTimes; retryTimes++) {
                    Long batchId = null;
                    boolean ackSuccess = false;

                    try {
                        final Message message = canalConnector.getWithoutAck(cksProps.getCanal().getBatchSize(), cksProps.getCanal().getFetchTimeOutInMills(), TimeUnit.MILLISECONDS);
                        batchId = message.getId();
                        if (batchId != -1 && CollectionUtils.isNotEmpty(message.getEntries())) {
                            syncToKudu(message);
                        }
                        canalConnector.ack(batchId);
                        ackSuccess = true;
                    } catch (Exception e) {
                        if (batchId != null) {
                            canalConnector.rollback(batchId);
                        }
                        log.error("doConsume retry error", e);
                    }

                    if (ackSuccess) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("doConsume error", e);
        } finally {
            canalConnector.unsubscribe();
            canalConnector.disconnect();
        }

    }

    /**
     * 多表多线程sync,不保证binlog顺序
     *
     * @param message
     */
    private void syncToKudu(Message message) throws ExecutionException, InterruptedException {
        if (message == null || CollectionUtils.isEmpty(message.getEntries())) {
            return;
        }

        final Map<String, List<CanalEntry.Entry>> srcTableIdMapCanalEntries = message.getEntries().stream()
                .filter(entry -> entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN
                        && entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND
                )
                .collect(Collectors.groupingBy(this::buildSrcTableId));


        final List<CompletableFuture<Void>> futures = srcTableIdMapCanalEntries.entrySet().stream()
                .map(entry -> {
                    final String srcTableId = entry.getKey();
                    final List<CanalEntry.Entry> canalEntries = entry.getValue();

                    return CompletableFuture.runAsync(() -> {
                        for (CanalEntry.Entry canalEntry : canalEntries) {
                            CanalEntry.RowChange rowChange;
                            try {
                                rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
                            } catch (Exception e) {
                                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                            }

                            if (rowChange.getIsDdl()) {
                                continue;
                            }

                            final CanalEntry.EventType eventType = rowChange.getEventType();

                            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                            }
                        }
                    }, cksTaskExecutor);
                })
                .collect(Collectors.toList());

        for (CompletableFuture<Void> future : futures) {
            future.get();
        }


        //TODO 按表名分组,互不影响
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
            ) {
                continue;
            }
            final String schemaName = entry.getHeader().getSchemaName();
            final String tableName = entry.getHeader().getTableName();
//TODO 需要通过schema+table,获取kudu中对应映射的表名
//TODO 如果kudu中不存在该表,则忽略,需要手动补数据
//            TODO 执行sync之前,需要查看该表目前是否可同步

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            if (rowChange.getIsDdl()) {
                continue;
            }

            final CanalEntry.EventType eventType = rowChange.getEventType();


            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                switch (eventType) {
                    case INSERT:
                        final List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                        for (CanalEntry.Column column : afterColumnsList) {
                            final Type type = columnNameMapType.get(column.getName());
                            switch (type) {
                                case INT8:
                                case INT16:
                                case INT32:
                                case INT64:
                                    break;
                                case BINARY:
                                    break;
                                case STRING:
                                    break;
                                case BOOL:
                                    break;
                                case FLOAT:
                                    break;
                                case DOUBLE:
                                    break;
                                case UNIXTIME_MICROS:
                                    break;
                                case DECIMAL:
                                    break;
                                default:
                                    throw new RuntimeException("not support kudu type: " + type);
                            }
                        }
                        break;
                    case UPDATE:
                        break;
                    case DELETE:
                        break;
                    default:
                        continue;
                }
            }

        }
    }

    private String buildSrcTableId(CanalEntry.Entry entry) {
        return entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
    }

}
