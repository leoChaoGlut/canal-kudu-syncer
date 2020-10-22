package personal.leo.cks.server.config;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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

    @Bean
    public ZkClientx zkClientx(CksProps cksProps) {
        final CksProps.Zk zkProps = cksProps.getZk();
        return ZkClientx.getZkClient(zkProps.getServers());
    }

    @Bean
    public TaskExecutor cksTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        final int corePoolSize = Runtime.getRuntime().availableProcessors();
        taskExecutor.setCorePoolSize(corePoolSize);
        taskExecutor.setMaxPoolSize(corePoolSize * 100);
        taskExecutor.setQueueCapacity(corePoolSize * 200);
        taskExecutor.initialize();
        return taskExecutor;
    }

}
