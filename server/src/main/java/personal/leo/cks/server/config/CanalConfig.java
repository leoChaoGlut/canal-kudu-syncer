package personal.leo.cks.server.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CanalProps;
import personal.leo.cks.server.constants.PropKey;

@Configuration
public class CanalConfig {

    @Bean
    @ConfigurationProperties(PropKey.CANAL)
    public CanalProps canalProps() {
        return new CanalProps();
    }

    @Bean
    public CanalConnector canalConnector(CanalProps canalProps) {
        return CanalConnectors.newClusterConnector(canalProps.getZkServers(), canalProps.getDestination(), canalProps.getUsername(), canalProps.getPassword());
    }
}
