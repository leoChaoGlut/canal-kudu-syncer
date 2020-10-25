package personal.leo.cks.server.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import personal.leo.cks.server.mapper.po.TableMappingInfo;

import java.util.List;

@Mapper
public interface TableMappingInfoMapper {

    @Select("select * from table_mapping_info")
    List<TableMappingInfo> selectAll();

}
