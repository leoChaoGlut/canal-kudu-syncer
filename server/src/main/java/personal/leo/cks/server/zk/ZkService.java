package personal.leo.cks.server.zk;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.constants.PropKey;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.util.List;

@Slf4j
@Component
public class ZkService {
    //    @Autowired
//    ZooKeeper zooKeeper;
    @Autowired
    ZkClientx zkClientx;
    @Value("${server.port}")
    private int port;

    public static final String rootPath = "/" + PropKey.CKS;
    public static final String masterPath = rootPath + "/master";
    public static final String availablePath = rootPath + "/available";

    String thisServer;
    String thisServerZkPath;

    @Getter
    boolean isMaster = false;

    @PostConstruct
    public void postConstruct() throws Exception {
        thisServer = InetAddress.getLocalHost().getHostAddress() + ":" + port;
        thisServerZkPath = availablePath + "/" + thisServer;

        tryToBuildZkFileStructure();
        markAvailable();
        tryToBeMaster();
    }

    private void tryToBeMaster() {
        isMaster = false;
        try {
            final List<String> availableServers = zkClientx.getChildren(availablePath);
            if (CollectionUtils.isNotEmpty(availableServers)) {
                final Stat stat = new Stat();
                final String masterServer = zkClientx.readData(masterPath, stat);
                if (!availableServers.contains(masterServer)) {
                    zkClientx.writeData(masterPath, thisServer, stat.getVersion());
                    isMaster = true;
                }
            }
        } catch (Exception e) {
            log.error("tryToBeMaster error", e);
        }
    }

    private void tryToBuildZkFileStructure() {
        try {
            zkClientx.createPersistent(masterPath, true);
        } catch (Exception e) {
            log.error("create masterPath error", e);
        }
        try {
            zkClientx.createPersistent(availablePath, true);
        } catch (Exception e) {
            log.error("create availablePath error", e);
        }
    }


    private void markAvailable() {
        zkClientx.delete(thisServerZkPath);
        zkClientx.createEphemeral(thisServerZkPath);
    }


}
