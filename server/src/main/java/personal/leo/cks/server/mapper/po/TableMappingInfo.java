package personal.leo.cks.server.mapper.po;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class TableMappingInfo {
    private String src_schema_name;
    private String src_table_name;
    private String kudu_table_name;
}
