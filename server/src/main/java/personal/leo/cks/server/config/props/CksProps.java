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
        /**
         * 不可大于 org.apache.kudu.client.AsyncKuduSession#mutationBufferMaxOps,否则kuduclient会报错
         *
         * @see AsyncKuduSession#apply(org.apache.kudu.client.Operation)
         * @see org.apache.kudu.client.AsyncKuduSession#mutationBufferMaxOps
         */
        private int maxBatchSize = 10000;
        private int syncPeriodMs = 500;
        /**
         * TODO 如果kudu库中没有该条数据,会导致插入数据遗漏,因为会比较源库中的before和after,只会找到变更的值
         */
        private boolean onlySyncValueChangedColumns = false;
    }
}
