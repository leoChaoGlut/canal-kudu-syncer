package personal.leo.cks.server.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CksProps;

@Configuration
public class CanalConfig {

    @Bean
    public CanalConnector canalConnector(CksProps cksProps) {
        final CksProps.Canal canalProps = cksProps.getCanal();
        final String zkServers = StringUtils.isBlank(canalProps.getZkServers()) ? cksProps.getZk().getServers() : canalProps.getZkServers();
        return CanalConnectors.newClusterConnector(zkServers, canalProps.getDestination(), canalProps.getUsername(), canalProps.getPassword());
    }
}
