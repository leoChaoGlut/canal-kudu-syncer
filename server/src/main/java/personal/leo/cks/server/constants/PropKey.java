package personal.leo.cks.server.constants;

public interface PropKey {
    String DOT = ".";
    String CKS_PREFIX = "canal-kudu-syncer";

    String HEALTH_THRESHOLD_IN_SEC_SUFFIX = "healthCheckPeriodInSec";
    String HEALTH_THRESHOLD_IN_SEC = CKS_PREFIX + DOT + HEALTH_THRESHOLD_IN_SEC_SUFFIX;

    String CANAL_SUFFIX = "canal";
    String CANAL = CKS_PREFIX + DOT + CANAL_SUFFIX;
}
