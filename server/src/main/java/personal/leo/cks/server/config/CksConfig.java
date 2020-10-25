package personal.leo.cks.server.config;

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


}
