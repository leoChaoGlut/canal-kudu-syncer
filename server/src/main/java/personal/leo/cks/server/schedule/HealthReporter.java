package personal.leo.cks.server.schedule;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.config.props.CksProps;
import personal.leo.cks.server.constants.PropKey;
import personal.leo.cks.server.util.IpUtils;
import personal.leo.cks.server.util.StateHolder;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO 如果mysql 阻塞,可能出现reportHealth不及时,后续考虑zk
 */
@Slf4j
@Component
public class HealthReporter {

    @Autowired
    CksProps cksProps;

    @Value("${server.port}")
    int port;

    final AtomicInteger reportHealthFailedCounter = new AtomicInteger(0);

    @Scheduled(fixedDelayString = "${" + PropKey.HEALTH_THRESHOLD_IN_SEC + "}")
    public void reportHealth() {
        try {
            doReportHealth();
        } catch (Exception e) {
            log.error("tryToBeMaster error", e);
        }
    }

    private void doReportHealth() {
        if (StateHolder.isInactive()) {
            return;
        }

        final String ip = IpUtils.getIp();
//        final int count = masterMapper.reportHealth(ip, port);
//        if (count != 1) {
//            final int reportHealthFailedCount = reportHealthFailedCounter.incrementAndGet();
//            if (reportHealthFailedCount > cksProps.calcHealthThresholdInSec() / 2) {
//                StateHolder.stateRef.set(State.INACTIVE);
//            }
//            throw new RuntimeException("report health failed");
//        }
    }
}
