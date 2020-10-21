package personal.leo.cks.server.util;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

public class NtpUtils {
    public static final String DEFAULT_NTP_SERVER = "ntp.aliyun.com";

    public static Date now(String ntpServer, int timeout) throws IOException {
        NTPUDPClient client = new NTPUDPClient();
        client.setDefaultTimeout(timeout);
        TimeInfo info = client.getTime(InetAddress.getByName(ntpServer));
        return new Date(info.getReturnTime());
    }
}
