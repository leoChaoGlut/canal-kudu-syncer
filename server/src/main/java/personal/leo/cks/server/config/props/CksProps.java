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
    private HealthCheck healthCheck;
    private Canal canal;

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
        private String server = "ntp.aliyun.com";
        private int timeoutInMills = 3000;
    }

    @Getter
    @Setter
    public static class HealthCheck {
        /**
         * 健康检查周期
         */
        private int periodInMills = 1000;
        /**
         * 健康检查容忍次数
         * 经过 healthCheckPeriodInSec*healthCheckToleranceTimes后,master的lastUpdateTime还没有更新的话,master会被认为已经失效
         */
        private int toleranceTimes = 20;

        public int calcHealthThresholdInSec() {
            final int healthThresholdInSec = periodInMills * toleranceTimes;
            if (healthThresholdInSec <= 0) {
                throw new RuntimeException("healthThresholdInSec is less than 0");
            }
            return healthThresholdInSec;
        }
    }

    @Getter
    @Setter
    public static class Canal {
        private String zkServers;
        private String destination;
        private String username;
        private String password;
    }
}
