-- Dataset used here is from: https://www.kaggle.com/datasets/ankurnapa/brewery-operations-and-market-analysis-dataset/

-- Table options
create or replace procedure `brewery`.create_table_options_sql(table_schema string, table_name string, out table_options_sql string)
begin

  set table_options_sql = (select concat("options(", options_agg, ")") from (
    select string_agg(c) options_agg from (
    select concat(option_name, "=", option_value) c
    from `region-eu`.INFORMATION_SCHEMA.TABLE_OPTIONS 
    where table_schema = table_schema and table_name = table_name
    )
  )
  );

end;

-- Table column definition with description
create or replace procedure `brewery`.create_columns_sql(table_schema string, table_name string, out columns_sql string)
begin
  set columns_sql = (select string_agg(col_with_options) from (
    select 
      concat(
        concat(concat(c1, " ", data_type, " DEFAULT ", column_default)),
        " options(",
        if(description is not null, concat("description=", "\"", description, "\","), "description=null,"),
        if(rounding_mode is not null, concat("rounding_mode=", rounding_mode, "\","), "rounding_mode=null"),
        ")\n"
      ) as col_with_options
    from (
      (select column_name c1, data_type, column_default
      from `region-eu`.INFORMATION_SCHEMA.COLUMNS 
      where
        table_schema = table_schema
        and table_name = table_name
      order by ordinal_position asc) t1
      left join
      (select column_name c2, description, rounding_mode 
      from `region-eu`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
      where
        table_schema = table_schema
        and table_name = table_name
      ) t2
      on t1.c1 = t2.c2
    )
  ));
end;

create or replace procedure common.create_partitioned_table()
begin
  declare old_dataset_name default "brewery";
  declare old_table_name default "brewery_ops";
  declare new_dataset_name default "brewery";
  declare new_table_name default "brewery_partitioned_clustered";

  declare create_partitioned_table_sql default "";
  declare drop_table_sql default "";
  declare columns_sql default "";
  declare table_options_sql default "";
  declare partition_column default "Brew_Date";
  declare cluster_column_list default "Location";
  declare select_old_table_data_sql default "as select * from `{old_dataset_name}`.`{old_table_name}`";

  set drop_table_sql = """drop table if exists `""" || new_dataset_name || """`.`""" || new_table_name || """`""";
  execute immediate drop_table_sql;

  set create_partitioned_table_sql = "create or replace table `{new_dataset_name}`.`{new_table_name}` ({columns_sql}) partition by {partition_column} cluster by {cluster_column_list} {table_options_sql} {select_old_table_data_sql}";
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{new_dataset_name}", new_dataset_name);
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{new_table_name}", new_table_name);

  call brewery.create_columns_sql(old_dataset_name, old_table_name, columns_sql);
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{columns_sql}", columns_sql);

  set partition_column = concat("date(", partition_column, ")");
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{partition_column}", partition_column);

  set cluster_column_list = "Location";
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{cluster_column_list}", cluster_column_list);

  call brewery.create_table_options_sql(old_dataset_name, old_table_name, table_options_sql);
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{table_options_sql}", table_options_sql);

  set select_old_table_data_sql = replace(select_old_table_data_sql, "{old_dataset_name}", old_dataset_name);
  set select_old_table_data_sql = replace(select_old_table_data_sql, "{old_table_name}", old_table_name);
  set create_partitioned_table_sql = replace(create_partitioned_table_sql, "{select_old_table_data_sql}", select_old_table_data_sql);

  execute immediate create_partitioned_table_sql;
end;

call common.create_partitioned_table();