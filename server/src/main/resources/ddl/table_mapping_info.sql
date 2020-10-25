create table test.table_mapping_info
(
    src_schema_name varchar(255) not null,
    src_table_name  varchar(255) not null,
    kudu_table_name varchar(255) not null,
    constraint table_mapping_info_ssn_stn_ktn_uindex
        unique (src_schema_name, src_table_name, kudu_table_name)
);

