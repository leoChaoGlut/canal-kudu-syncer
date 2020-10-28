package personal.leo.cks.server.constants;

public interface ZkPath {
    String rootPath = "/" + PropKey.CKS;
    String tableMappingInfo = rootPath + "/table-mapping-info";
    String taskList = rootPath + "/task/list";
    String taskRunning = rootPath + "/task/running";
}
