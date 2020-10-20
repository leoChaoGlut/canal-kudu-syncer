package personal.leo.cks.server.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CksProps;

@Configuration
public class CanalConfig {

    @Bean
    public CanalConnector canalConnector(CksProps cksProps) {
        final CksProps.Canal canalProps = cksProps.getCanal();
        return CanalConnectors.newClusterConnector(canalProps.getZkServers(), canalProps.getDestination(), canalProps.getUsername(), canalProps.getPassword());
    }
}
