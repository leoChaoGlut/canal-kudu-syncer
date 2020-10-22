package personal.leo.cks.server.zk;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import personal.leo.cks.server.constants.PropKey;

import javax.annotation.PostConstruct;
import java.net.InetAddress;

@Slf4j
@Component
public class ZkService {

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

        subscribeAvailableServerChange();

        subscribeSessionStateChange();

        markAvailable();
    }

    private void subscribeSessionStateChange() {
        zkClientx.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                try {
                    isMaster = false;
                    if (state == Watcher.Event.KeeperState.SyncConnected) {
                        markAvailable();
                    }
                } catch (Exception e) {
                    log.error("handleStateChanged error", e);
                }
            }

            @Override
            public void handleNewSession() throws Exception {
                log.info("handleNewSession");
            }

            @Override
            public void handleSessionEstablishmentError(Throwable error) throws Exception {
                log.error("handleSessionEstablishmentError", error);
                isMaster = false;
            }
        });
    }

    private void subscribeAvailableServerChange() {
        zkClientx.subscribeChildChanges(availablePath, (parentPath, availableServers) -> {
            try {
                if (CollectionUtils.isEmpty(availableServers)) {
                    isMaster = false;
                    markAvailable();
                } else {
                    final Stat oldMasterStat = new Stat();
                    final String masterServer = zkClientx.readData(masterPath, oldMasterStat);
                    if (availableServers.contains(masterServer)) {
                        isMaster = StringUtils.equals(thisServer, masterServer);
                    } else {
                        zkClientx.writeData(masterPath, thisServer, oldMasterStat.getVersion());
                        isMaster = true;
                    }
                }
            } catch (Exception e) {
                log.error("subscribeChildChanges error", e);
            }
        });
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
