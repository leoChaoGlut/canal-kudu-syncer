package personal.leo.cks.server.consume;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import personal.leo.cks.server.zk.ZkService;

import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalTcpConsumer {

    @Autowired
    ZkService zkService;

    public void consumer() {
        while (true) {
            if (zkService.isMaster()) {
//                TODO
            } else {
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    log.error("sleep error", e);
                }
            }
        }
    }
}
