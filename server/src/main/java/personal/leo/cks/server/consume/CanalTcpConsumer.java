package personal.leo.cks.server.consume;

import lombok.extern.slf4j.Slf4j;
import personal.leo.cks.server.util.StateHolder;

import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalTcpConsumer {

    boolean running;

    public void consumer() {
        while (running) {
            if (StateHolder.isActive()) {
//TODO consume
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
