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
    private Canal canal;
    private Kudu kudu;

    @Getter
    @Setter
    public static class Zk {
        /**
         * split by ,
         */
        private String servers;
    }

    @Getter
    @Setter
    public static class Canal {
        /**
         * split by ,
         */
        private String zkServers;
        private String destination;
        private String username;
        private String password;
        private int batchSize = 1000;
        private long fetchTimeOutMs = 500L;
    }

    @Getter
    @Setter
    public static class Kudu {
        /**
         * split by ,
         */
        private String masterAddresses;
    }
}
