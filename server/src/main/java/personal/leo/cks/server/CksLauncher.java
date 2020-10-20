package personal.leo.cks.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * cks: Canal Kudu Syncer
 */
@SpringBootApplication
public class CksLauncher {
    public static void main(String[] args) {
        try {
            SpringApplication.run(CksLauncher.class, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
