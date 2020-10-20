package personal.leo.cks.server.config.props;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CksProps {
    /**
     * host:port;host:port;
     */
    private Zk zk;
    private Ntp ntp;
    /**
     * 健康检查周期
     */
    private int healthCheckPeriodInSec = 1;
    /**
     * 健康检查容忍次数
     * 经过 healthCheckPeriodInSec*healthCheckToleranceTimes后,master的lastUpdateTime还没有更新的话,master会被认为已经失效
     */
    private int healthCheckToleranceTimes = 20;

    public int calcHealthThresholdInSec() {
        final int healthThresholdInSec = healthCheckPeriodInSec * healthCheckToleranceTimes;
        if (healthThresholdInSec <= 0) {
            throw new RuntimeException("healthThresholdInSec is less than 0");
        }
        return healthThresholdInSec;
    }

    @Getter
    @Setter
    public static class Zk {
        private String servers;
        private int timeoutInMills = 3000;
    }

    @Getter
    @Setter
    public static class Ntp {
        private boolean enabled;
        private String server;
        private int timeoutInMills = 3000;
    }

    @Getter
    @Setter
    public static class HealthCheck {
        private boolean enabled;
        private String server;
        private int timeoutInMills = 3000;
    }
}
