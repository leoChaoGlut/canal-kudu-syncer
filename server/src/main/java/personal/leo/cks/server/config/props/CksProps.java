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

    @Getter
    @Setter
    public static class Zk {
        /**
         * split by ,
         */
        private String servers;
        private int timeoutInMills = 3000;
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
    }
}
