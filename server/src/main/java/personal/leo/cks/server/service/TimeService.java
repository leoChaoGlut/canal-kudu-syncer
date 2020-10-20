package personal.leo.cks.server.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import personal.leo.cks.server.config.props.CksProps;

import java.util.Date;

@Service
public class TimeService {
    @Autowired
    CksProps cksProps;

    public Date now() {
//        if (ntpEnabled) {
//            NtpUtils.now()
//        } else {
//
//        }
        return null;
    }

}
