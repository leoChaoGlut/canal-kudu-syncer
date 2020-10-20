package personal.leo.cks.server.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpUtils {
    /**
     * TODO 不同网络下,可能需要的ip不一样,需要改变一下这里
     *
     * @return
     * @throws UnknownHostException
     */
    public static String getIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
