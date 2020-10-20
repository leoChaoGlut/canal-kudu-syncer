package personal.leo.cks.server.schedule;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.config.props.CksProps;

import javax.annotation.PostConstruct;

/**
 * 检查master的健康状况
 * 这里使用mysql作为一致性保障,如果对可用性要求较高,可改写使用zk实现
 */
@Slf4j
@Component
public class MasterHealthChecker {
    @Autowired
    CksProps cksProps;
//    @Autowired
//    MasterMapper masterMapper;

    @Value("${server.port}")
    int port;

    @PostConstruct
    public void PostConstruct() {

    }


    @Scheduled(fixedDelayString = "${canal-kudu-syncer.healthCheck.periodInSec}")
    public void tryToBeMaster() {
        try {
            doTryToBeMaster();
        } catch (Exception e) {
            log.error("tryToBeMaster error", e);
        }
    }

    private void doTryToBeMaster() {
//        final int healthThresholdInSec = cksProps.calcHealthThresholdInSec();
//        final MasterPO inactivedMaster = masterMapper.selectInactived(healthThresholdInSec);
//        if (inactivedMaster != null) {
//            final String ip = IpUtils.getIp();
//            final int count = masterMapper.tryToBeMaster(ip, port, inactivedMaster.getHost(), inactivedMaster.getPort());
//            if (count != 1) {
//                throw new RuntimeException("tryToBeMaster failed");
//            }
//            StateHolder.stateRef.set(State.ACTIVE);
//        }
    }


}
