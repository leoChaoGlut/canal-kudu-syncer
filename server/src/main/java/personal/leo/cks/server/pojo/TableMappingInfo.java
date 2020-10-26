package personal.leo.cks.server.pojo;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import personal.leo.cks.server.util.IdUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Setter
@Accessors(chain = true)
@EqualsAndHashCode
public class TableMappingInfo {
    public static final String COMMA = ",";

    private String srcSchemaName;
    private String srcTableName;
    private String kuduTableName;

    public static Set<TableMappingInfo> parse(String csv) {
        if (StringUtils.isBlank(csv)) {
            return new HashSet<>();
        }

        final String[] rows = StringUtils.splitByWholeSeparator(csv, "\n");
        if (ArrayUtils.isEmpty(rows)) {
            return new HashSet<>();
        }

        return Arrays.stream(rows)
                .map(row -> {
                    final String[] columns = StringUtils.splitByWholeSeparator(row, COMMA);
                    if (ArrayUtils.isEmpty(columns) || columns.length != 3) {
                        return null;
                    }
                    return columns;
                })
                .filter(Objects::nonNull)
                .map(columns -> {
                    final String srcSchemaName = columns[0];
                    final String srcTableName = columns[1];
                    final String kuduTableName = columns[2];
                    return new TableMappingInfo()
                            .setSrcSchemaName(srcSchemaName)
                            .setSrcTableName(srcTableName)
                            .setKuduTableName(kuduTableName);
                })
                .collect(Collectors.toSet());
    }

    public static String toCsv(Set<TableMappingInfo> tableMappingInfos) {
        if (CollectionUtils.isEmpty(tableMappingInfos)) {
            return null;
        }
        return tableMappingInfos.stream()
                .map(tableMappingInfo -> tableMappingInfo.getSrcSchemaName() + COMMA + tableMappingInfo.getSrcTableName() + COMMA + tableMappingInfo.getKuduTableName())
                .collect(Collectors.joining("\n"));
    }

    public String fetchSrcTableId() {
        return srcSchemaName + IdUtils.DOT + srcTableName;
    }
}
