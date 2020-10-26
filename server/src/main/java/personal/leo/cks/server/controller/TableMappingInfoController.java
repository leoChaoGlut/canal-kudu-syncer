package personal.leo.cks.server.controller;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import personal.leo.cks.server.constants.ZkPath;
import personal.leo.cks.server.pojo.TableMappingInfo;
import personal.leo.cks.server.service.CuratorService;

import java.util.Set;

@RestController
@RequestMapping("tableMappingInfo")
public class TableMappingInfoController {
    @Autowired
    CuratorService curatorService;

    @GetMapping("add")
    public void add(String srcSchemaName, String srcTableName, String kuduTableName) throws Exception {
        final Stat stat = new Stat();
        final Set<TableMappingInfo> tableMappingInfos = fetchTableMappingInfos(stat);
        final TableMappingInfo tableMappingInfo = new TableMappingInfo().setSrcSchemaName(srcSchemaName).setSrcTableName(srcTableName).setKuduTableName(kuduTableName);
        if (tableMappingInfos.contains(tableMappingInfo)) {
            throw new RuntimeException("exists: " + tableMappingInfo);
        } else {
            tableMappingInfos.add(tableMappingInfo);
            curatorService.writeData(ZkPath.tableMappingInfo, TableMappingInfo.toCsv(tableMappingInfos), stat.getVersion());
        }
    }

    @GetMapping("list")
    public Set<TableMappingInfo> list() throws Exception {
        final String csv = curatorService.readData(ZkPath.tableMappingInfo);
        return TableMappingInfo.parse(csv);
    }

    @GetMapping("remove")
    public void remove(String srcSchemaName, String srcTableName, String kuduTableName) throws Exception {
        final Stat stat = new Stat();
        final Set<TableMappingInfo> tableMappingInfos = fetchTableMappingInfos(stat);
        final TableMappingInfo tableMappingInfo = new TableMappingInfo().setSrcSchemaName(srcSchemaName).setSrcTableName(srcTableName).setKuduTableName(kuduTableName);
        if (tableMappingInfos.contains(tableMappingInfo)) {
            tableMappingInfos.remove(tableMappingInfo);
            curatorService.writeData(ZkPath.tableMappingInfo, TableMappingInfo.toCsv(tableMappingInfos), stat.getVersion());
        } else {
            throw new RuntimeException("not exists: " + tableMappingInfo);
        }
    }

    private Set<TableMappingInfo> fetchTableMappingInfos(Stat stat) throws Exception {
        final String csv = curatorService.readData(ZkPath.tableMappingInfo, stat);
        return TableMappingInfo.parse(csv);
    }

    @GetMapping("update")
    public void update(String srcSchemaName, String srcTableName, String newKuduTableName) throws Exception {
        final Stat stat = new Stat();
        final Set<TableMappingInfo> tableMappingInfos = fetchTableMappingInfos(stat);
        for (TableMappingInfo tableMappingInfo : tableMappingInfos) {
            if (StringUtils.equals(tableMappingInfo.getSrcSchemaName(), srcSchemaName)
                    && StringUtils.equals(tableMappingInfo.getSrcTableName(), srcTableName)
            ) {
                tableMappingInfo.setKuduTableName(newKuduTableName);
            }
        }
        curatorService.writeData(ZkPath.tableMappingInfo, TableMappingInfo.toCsv(tableMappingInfos), stat.getVersion());
    }
}
