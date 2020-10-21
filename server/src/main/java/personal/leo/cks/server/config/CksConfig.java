package personal.leo.cks.server.config;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CksProps;
import personal.leo.cks.server.constants.PropKey;

@Slf4j
@Configuration
public class CksConfig {
    @Bean
    @ConfigurationProperties(PropKey.CKS)
    public CksProps cksProps() {
        return new CksProps();
    }

//    @Bean
//    public ZooKeeper zooKeeper(CksProps cksProps) throws IOException {
//        final CksProps.Zk zkProps = cksProps.getZk();
//        return new ZooKeeper(zkProps.getServers(), zkProps.getTimeoutInMills(), event -> log.info(event.toString()));
//    }

    @Bean
    public ZkClientx zkClientx(CksProps cksProps) {
        final CksProps.Zk zkProps = cksProps.getZk();
        return ZkClientx.getZkClient(zkProps.getServers());
    }

}
