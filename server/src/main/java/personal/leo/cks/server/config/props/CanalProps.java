package personal.leo.cks.server.config.props;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CanalProps {
    private String zkServers;
    private String destination;
    private String username;
    private String password;
}
