package personal.leo.cks.server.service;

import com.alibaba.otter.canal.client.CanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import personal.leo.cks.server.constants.ZkPath;
import personal.leo.cks.server.pojo.TableMappingInfo;
import personal.leo.cks.server.util.IdUtils;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    @Autowired
    private CuratorFramework curator;

    private final ConcurrentHashMap<String, Properties> taskIdMapProps = new ConcurrentHashMap<>();
    private final Map<String, ColumnSchema> kuduColumnIdMapKuduColumn = Collections.synchronizedMap(new HashedMap<>());
    private final Map<String, String> srcTableIdMapKuduTableName = Collections.synchronizedMap(new HashedMap<>());
    private final Set<String> runningTaskIds = Collections.synchronizedSet(new HashSet<>());
    private final String thisServerId = UUID.randomUUID().toString();

    @PostConstruct
    private void postConstruct() throws Exception {

//        curatorService.addTreeCacheListener(ZkPath.tableMappingInfo, (client, event) -> {
//            if (event.getData() != null) {
//                reloadKuduColumnIdMapKuduColumnType();
//                reloadSrcTableIdMapKuduTableName(event.getData().getData());
//            }
//        });

        taskListListener();

        taskRunningListener();
    }

    private void taskRunningListener() throws Exception {
        curatorService.addTreeCacheListener(ZkPath.taskRunning, (client, event) -> {
            final ChildData eventData = event.getData();
            if (eventData == null) {
                return;
            }
            final String path = eventData.getPath();
            final String taskId = StringUtils.substring(StringUtils.substringAfter(path, ZkPath.taskList), 1);
            final String taskServerId = new String(eventData.getData());
            if (StringUtils.isBlank(taskId)) {//root path
//                TODO
            } else {
                switch (event.getType()) {
                    case NODE_ADDED:
//                        startTask(taskId, taskServerId);
                        break;
                    case NODE_UPDATED:
                        break;
                    case NODE_REMOVED:
                        break;
                }
            }


        });
    }

    private void taskListListener() throws Exception {
        curatorService.addTreeCacheListener(ZkPath.taskList, (client, event) -> {
            final ChildData eventData = event.getData();
            if (eventData == null) {
                return;
            }
            final String path = eventData.getPath();
            final String taskId = StringUtils.substring(StringUtils.substringAfter(path, ZkPath.taskList), 1);
            if (StringUtils.isBlank(taskId)) {
                //root path
                return;
            }

            final Stat stat = eventData.getStat();
            final Properties props = new Properties();
            props.load(new ByteArrayInputStream(eventData.getData()));

            switch (event.getType()) {
                case NODE_ADDED:
                    taskIdMapProps.put(taskId, props);
                    tryToClaimTask(taskId);
                    break;
                case NODE_UPDATED:
                    taskIdMapProps.put(taskId, props);
                    restartTask(taskId, props, stat);
                    break;
                case NODE_REMOVED:
                    taskIdMapProps.remove(taskId);
                    stopTask();
                    break;
            }
        });
    }

    private void startTask(String taskId, String taskServerId) {
        if (StringUtils.equals(taskServerId, thisServerId)) {
            runningTaskIds.add(taskId);
//            TODO start task
        }
    }

    private void stopTask() {
    }

    private void restartTask(String taskId, Properties props, Stat stat) throws Exception {
        final List<String> runningTaskIds = curator.getChildren().forPath(ZkPath.taskRunning);
        if (CollectionUtils.isEmpty(runningTaskIds)) {
            tryToClaimTask(taskId);
        } else {
        }
    }

    private void tryToClaimTask(String taskId) throws Exception {
        final String taskPath = ZkPath.taskRunning + "/" + taskId;
        final Stat stat = new Stat();
        curator.create().storingStatIn(stat).withMode(CreateMode.EPHEMERAL).forPath(taskPath, thisServerId.getBytes(StandardCharsets.UTF_8));
        runningTaskIds.add(taskId);
        try {
            //TODO start maxwell
        } catch (Exception e) {
            log.error("start maxwell error", e);
            curator.delete().withVersion(stat.getVersion()).forPath(taskPath);
        }
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
