package personal.leo.cks.server.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kudu.client.KuduException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.config.props.CksProps;
import personal.leo.cks.server.exception.FatalException;
import personal.leo.cks.server.kudu.KuduSyncer;
import personal.leo.cks.server.kudu.OperationType;

import javax.annotation.PostConstruct;
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

    @PostConstruct
    private void postConstruct() {
        consume();
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
            sleepSec(1);
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
                final StopWatch watch = StopWatch.createStarted();
                final boolean isStandByClient = !canalConnector.checkValid();
                watch.stop();
                log.info("checkValid spend: " + watch);
                if (isStandByClient) {
                    sleepSec(3);
                    continue;
                }

                long batchId = INVALID_BATCH_ID;
                KuduSyncer.SyncError syncError = null;

                try {
                    watch.reset();
                    watch.start();
                    syncError = kuduSyncer.getSyncError();
                    watch.stop();
                    log.info("getSyncError spend: " + watch);
                    if (syncError != null) {
                        throw syncError.getException();
                    }
                    watch.reset();
                    watch.start();
                    final Message message = canalConnector.getWithoutAck(cksProps.getCanal().getBatchSize(), cksProps.getCanal().getFetchTimeOutMs(), TimeUnit.MILLISECONDS);
                    watch.stop();
                    log.info("getWithoutAck spend: " + watch);
                    batchId = message.getId();
                    if (batchId == INVALID_BATCH_ID || CollectionUtils.isEmpty(message.getEntries())) {
                        canalConnector.ack(batchId);
                        log.info("no msg:" + batchId);
                        sleepSec(1);
                    } else {
                        watch.reset();
                        watch.start();
                        syncToKudu(message);
                        watch.stop();
                        log.info("syncToKudu spend: " + watch);
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
                    sleepSec(1);
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


    private void sleepSec(long sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (InterruptedException e) {
            log.error("sleep error", e);
        }
    }
}
