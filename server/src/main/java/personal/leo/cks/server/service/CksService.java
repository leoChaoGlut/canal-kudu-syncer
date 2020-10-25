package personal.leo.cks.server.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import personal.leo.cks.server.mapper.TableMappingInfoMapper;
import personal.leo.cks.server.mapper.po.TableMappingInfo;
import personal.leo.cks.server.util.IdUtils;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * TODO 需要支持reload
 * TODO 定时更新
 *
 * @return
 * @throws KuduException
 */
@Slf4j
@Service
public class CksService {

    @Autowired
    private KuduClient kuduClient;
    @Autowired
    private TableMappingInfoMapper tableMappingInfoMapper;

    private Map<String, Type> kuduColumnIdMapKuduColumnType;
    private Map<String, String> srcTableIdMapKuduTableName;


    @PostConstruct
    private void postConstruct() throws KuduException {
        reloadKuduColumnIdMapKuduColumnType();
        reloadSrcTableIdMapKuduTableName();
    }


    public String getKuduTableName(String srcTableId) {
        return srcTableIdMapKuduTableName.get(srcTableId);
    }

    public Type getKuduColumnType(String kuduColumnId) {
        return kuduColumnIdMapKuduColumnType.get(kuduColumnId);
    }


    private void reloadKuduColumnIdMapKuduColumnType() throws KuduException {
        final StopWatch watch = StopWatch.createStarted();
        kuduColumnIdMapKuduColumnType = new HashedMap<>();
        for (String tableName : kuduClient.getTablesList().getTablesList()) {
            final KuduTable table = kuduClient.openTable(tableName);
            for (ColumnSchema column : table.getSchema().getColumns()) {
                final String kuduColumnId = IdUtils.buildKuduColumnId(table, column);
                kuduColumnIdMapKuduColumnType.put(kuduColumnId, column.getType());
            }
        }
        watch.stop();
        log.info("reloadKuduColumnIdMapKuduColumnType spend: " + watch);
    }


    private void reloadSrcTableIdMapKuduTableName() {
        final StopWatch watch = StopWatch.createStarted();
        srcTableIdMapKuduTableName = new HashedMap<>();

        for (TableMappingInfo tableMappingInfo : tableMappingInfoMapper.selectAll()) {
            final String srcTableId = IdUtils.buildSrcTableId(tableMappingInfo.getSrc_schema_name(), tableMappingInfo.getSrc_table_name());
            srcTableIdMapKuduTableName.put(srcTableId, tableMappingInfo.getKudu_table_name());
        }

        watch.stop();
        log.info("reloadSrcTableIdMapKuduTableName spend: " + watch);
    }
}
