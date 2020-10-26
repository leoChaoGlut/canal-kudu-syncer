package personal.leo.cks.server.service;

import com.alibaba.otter.canal.client.CanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import personal.leo.cks.server.constants.ZkPath;
import personal.leo.cks.server.pojo.TableMappingInfo;
import personal.leo.cks.server.util.IdUtils;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static personal.leo.cks.server.pojo.TableMappingInfo.COMMA;


@Slf4j
@Service
public class CksService {

    @Autowired
    private KuduClient kuduClient;
    @Autowired
    private CanalConnector canalConnector;
    @Autowired
    private CuratorService curatorService;

    private Map<String, ColumnSchema> kuduColumnIdMapKuduColumn = Collections.synchronizedMap(new HashedMap<>());
    private Map<String, String> srcTableIdMapKuduTableName = Collections.synchronizedMap(new HashedMap<>());

    @PostConstruct
    private void postConstruct() throws Exception {
        curatorService.addTreeCacheListener(ZkPath.tableMappingInfo, (client, event) -> {
            if (event.getData() != null) {
                reloadKuduColumnIdMapKuduColumnType();
                reloadSrcTableIdMapKuduTableName(event.getData().getData());
            }
        });
    }


    public String getKuduTableName(String srcTableId) {
        return srcTableIdMapKuduTableName.get(srcTableId);
    }

    public Type getKuduColumnType(String kuduColumnId) {
        final ColumnSchema columnSchema = kuduColumnIdMapKuduColumn.get(kuduColumnId);
        if (columnSchema == null) {
            return null;
        }
        return columnSchema.getType();
    }

    public ColumnSchema getKuduColumn(String kuduColumnId) {
        return kuduColumnIdMapKuduColumn.get(kuduColumnId);
    }

    private void reloadKuduColumnIdMapKuduColumnType() throws KuduException {
        final StopWatch watch = StopWatch.createStarted();
        kuduColumnIdMapKuduColumn.clear();

        for (String tableName : kuduClient.getTablesList().getTablesList()) {
            final KuduTable kuduTable = kuduClient.openTable(tableName);
            for (ColumnSchema kuduColumn : kuduTable.getSchema().getColumns()) {
                final String kuduColumnId = IdUtils.buildKuduColumnId(kuduTable, kuduColumn);
                kuduColumnIdMapKuduColumn.put(kuduColumnId, kuduColumn);
            }
        }
        watch.stop();
        log.info("reloadKuduColumnIdMapKuduColumnType spend: " + watch);
    }


    private void reloadSrcTableIdMapKuduTableName(byte[] bytes) {
        final StopWatch watch = StopWatch.createStarted();
        srcTableIdMapKuduTableName.clear();

        if (bytes == null) {
            return;
        }

        final String csv = new String(bytes, StandardCharsets.UTF_8);

        final Set<TableMappingInfo> tableMappingInfos = TableMappingInfo.parse(csv);

        final String subscribeFilter = tableMappingInfos.stream()
                .map(tableMappingInfo -> {
                    final String srcTableId = tableMappingInfo.fetchSrcTableId();
                    srcTableIdMapKuduTableName.put(srcTableId, tableMappingInfo.getKuduTableName());
                    return srcTableId;
                })
                .reduce((srcTableId1, srcTableId2) -> srcTableId1 + COMMA + srcTableId2)
                .orElse(null);

        //TODO 多实例的时候,需要测试这里是否会阻塞
        if (StringUtils.isNotBlank(subscribeFilter)) {
            log.info("subscribe filter changed: " + subscribeFilter);
            if (canalConnector.checkValid()) {
                log.info("change subscribe filter before: " + subscribeFilter);
                canalConnector.subscribe(subscribeFilter);
                log.info("change subscribe filter after: " + subscribeFilter);
            }
        }

        watch.stop();
        log.info("reloadSrcTableIdMapKuduTableName spend: " + watch);
    }
}
