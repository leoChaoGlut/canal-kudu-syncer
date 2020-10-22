package personal.leo.cks.server.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CksProps;

import java.util.Map;

@Slf4j
@Configuration
public class KuduConfig {

    @Bean
    public KuduClient kuduClient(CksProps cksProps) {
        final CksProps.Kudu kuduProps = cksProps.getKudu();
        return new KuduClient.KuduClientBuilder(kuduProps.getMasterAddresses()).build();
    }

    /**
     * TODO 改为immutable更合适
     * TODO 需要支持reload
     * TODO 定时更新
     *
     * @param kuduClient
     * @return
     * @throws KuduException
     */
    @Bean
    public Map<String, Type> columnNameMapType(KuduClient kuduClient) throws KuduException {
        final StopWatch watch = StopWatch.createStarted();
        final Map<String, Type> columnNameMapType = new HashedMap<>();
        for (String tableName : kuduClient.getTablesList().getTablesList()) {
            final KuduTable table = kuduClient.openTable(tableName);
            for (ColumnSchema column : table.getSchema().getColumns()) {
                columnNameMapType.put(column.getName(), column.getType());
            }
        }
        watch.stop();
        log.info("columnNameMapType spend: " + watch);
        return columnNameMapType;
    }

    @Bean
    public Map<String, Type> srcTableIdMap(KuduClient kuduClient) throws KuduException {
        final StopWatch watch = StopWatch.createStarted();
        final Map<String, Type> columnNameMapType = new HashedMap<>();
        for (String tableName : kuduClient.getTablesList().getTablesList()) {
            final KuduTable table = kuduClient.openTable(tableName);
            for (ColumnSchema column : table.getSchema().getColumns()) {
                columnNameMapType.put(column.getName(), column.getType());
            }
        }
        watch.stop();
        log.info("columnNameMapType spend: " + watch);
        return columnNameMapType;
    }
}