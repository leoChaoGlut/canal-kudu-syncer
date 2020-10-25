package personal.leo.cks.server.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.KuduClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.cks.server.config.props.CksProps;

@Slf4j
@Configuration
public class KuduConfig {

    @Bean
    public KuduClient kuduClient(CksProps cksProps) {
        final CksProps.Kudu kuduProps = cksProps.getKudu();
        return new KuduClient.KuduClientBuilder(kuduProps.getMasterAddresses()).build();
    }


}
