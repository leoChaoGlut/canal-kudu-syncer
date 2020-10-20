package personal.leo.cks.server.config;

import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CksProps;

import java.io.IOException;

@Configuration
public class CksConfig {
    @Bean
    @ConfigurationProperties("canal-kudu-syncer")
    public CksProps cksProps() {
        return new CksProps();
    }

    @Bean
    public ZooKeeper zooKeeper(CksProps cksProps) throws IOException {
        final CksProps.Zk zkProps = cksProps.getZk();
        return new ZooKeeper(zkProps.getServers(), zkProps.getTimeoutInMills(), System.out::println);
    }

}
