package personal.leo.cks.server.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.kudu.client.KuduException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.config.props.CksProps;
import personal.leo.cks.server.exception.FatalException;
import personal.leo.cks.server.kudu.KuduSyncer;
import personal.leo.cks.server.kudu.OperationType;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;

import static com.alibaba.otter.canal.protocol.CanalEntry.EntryType.TRANSACTIONBEGIN;
import static com.alibaba.otter.canal.protocol.CanalEntry.EntryType.TRANSACTIONEND;

@Slf4j
@Component
public class CanalTcpConsumer {

    private static final long INVALID_BATCH_ID = -1;

    @Autowired
    CanalConnector canalConnector;
    @Autowired
    CksProps cksProps;
    @Autowired
    KuduSyncer kuduSyncer;
    @Autowired
    CuratorFramework curator;

    @PostConstruct
    private void postConstruct() throws Exception {
        subscribeFilterChange();
        consume();
    }


    private void subscribeFilterChange() throws Exception {
        final String path = cksProps.getZk().getRootPath() + "/canal/subscribe";
        final TreeCache treeCache = TreeCache.newBuilder(curator, path).build();
        treeCache.getListenable().addListener((curatorFramework, treeCacheEvent) -> {
            final ChildData childData = treeCacheEvent.getData();
            if (childData != null) {
                final String filter = new String(childData.getData(), StandardCharsets.UTF_8);
                log.info("subscribe filter changed: " + filter);
                if (canalConnector.checkValid()) {
                    log.info("change subscribe filter before: " + filter);
                    canalConnector.subscribe(filter);
                    log.info("change subscribe filter after: " + filter);
                }
            }
        });
        treeCache.start();
    }

    /**
     * TODO 失败告警邮件
     * TODO client HA 有1.5min的延迟
     * TODO no alive canal server for i2
     */
    private void consume() {
        while (true) {
            try {
                doConsume();
            } catch (Exception e) {
                log.error("doConsumer error", e);
                FatalException.throwIfFatal(e);
            }
            sleepMs(1000);
        }
    }

    /**
     * TODO bootstrap完成后,动态subscribe
     */
    private void doConsume() {
        try {
            canalConnector.connect();
            log.info("after connect");
            canalConnector.subscribe();
            log.info("after subscribe");

            while (true) {
                long batchId = INVALID_BATCH_ID;
                KuduSyncer.SyncError syncError = null;

                try {
                    syncError = kuduSyncer.getSyncError();
                    if (syncError != null) {
                        throw syncError.getException();
                    }
                    final Message message = canalConnector.getWithoutAck(cksProps.getCanal().getBatchSize(), cksProps.getCanal().getFetchTimeOutMs(), TimeUnit.MILLISECONDS);
                    batchId = message.getId();
                    if (batchId == INVALID_BATCH_ID || CollectionUtils.isEmpty(message.getEntries())) {
                        canalConnector.ack(batchId);
                        sleepMs(500);
                    } else {
                        syncToKudu(message);
                        canalConnector.ack(batchId);
                    }
                } catch (Exception e) {
                    log.error("doConsume loop error", e);
                    if (batchId != INVALID_BATCH_ID) {
                        canalConnector.rollback(batchId);
                    }
                    if (syncError != null) {
                        syncError.getBatchIds().forEach(bId -> canalConnector.rollback(bId));
                    }
                    sleepMs(500);
                    FatalException.throwIfFatal(e);
                }
            }
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
    private void syncToKudu(Message message) throws KuduException, ParseException {
        if (message == null || CollectionUtils.isEmpty(message.getEntries())) {
            return;
        }

        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() == TRANSACTIONBEGIN || entry.getEntryType() == TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            if (rowChange.getIsDdl()) {
                continue;
            }

            final CanalEntry.EventType eventType = rowChange.getEventType();

            final long batchId = message.getId();

            switch (eventType) {
                case INSERT:
                    kuduSyncer.doOperation(entry, rowChange, OperationType.INSERT, batchId);
                    break;
                case UPDATE:
                    kuduSyncer.doOperation(entry, rowChange, OperationType.UPDATE, batchId);
                    break;
                case DELETE:
                    kuduSyncer.doOperation(entry, rowChange, OperationType.DELETE, batchId);
                    break;
            }
        }

    }


    private void sleepMs(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            log.error("sleep error", e);
        }
    }
}
