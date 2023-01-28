# Databricks notebook source
print("hello");

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into standardized.diy_cpl_dsm_tmp
# MAGIC    SELECT
# MAGIC     a.component_part_list_r_sid,
# MAGIC     a.from_part_sid,
# MAGIC     a.from_part_type,
# MAGIC     coalesce(b.float_part_sid, a.to_part_sid) to_part_sid,
# MAGIC     coalesce(b.type, a.to_part_type) to_part_type,
# MAGIC     CASE
# MAGIC         WHEN coalesce(b.type, a.to_part_type) != 'Formulation Phase' THEN a.loss
# MAGIC         ELSE CAST(NULL AS INT)
# MAGIC     END loss -- ignore loss on phases
# MAGIC     ,
# MAGIC     CASE
# MAGIC         WHEN coalesce(b.type, a.to_part_type) = 'Formulation Phase' THEN CASE
# MAGIC             WHEN a.from_part_type = 'Formulation Phase' THEN a.qty -- keep phase quantities for "Not Included Phase" (phase within a phase)
# MAGIC             ELSE CAST(NULL AS INT) -- ignore phase quantities when not a "Not Included Phase"
# MAGIC         END
# MAGIC         ELSE a.qty  -- keep quantities for everything not under phase
# MAGIC     END qty,
# MAGIC     -1 qty_uom_sid,
# MAGIC     null loss_factor_at_level,
# MAGIC       - 1 vlt_proc_id,
# MAGIC     current_timestamp() vlt_load_datetm,
# MAGIC     'COMPONENT_PART_LIST_R' vlt_src_tbl_name,
# MAGIC     CAST(a.component_part_list_r_sid AS string) vlt_src_tbl_pk_txt,
# MAGIC     a.vlt_src_tbl_name   vlt_src_tbl_name_s
# MAGIC FROM
# MAGIC     standardized.component_part_list_r
# MAGIC     LEFT OUTER JOIN (
# MAGIC         SELECT
# MAGIC             y.part_sid,
# MAGIC             y.float_part_sid,
# MAGIC             z.type
# MAGIC         FROM
# MAGIC             standardized.enovia_part_float_fs
# MAGIC             INNER JOIN standardized.part z ON ( z.vlt_delete_ind = 'N'
# MAGIC                                                 AND y.float_part_sid = z.part_sid )
# MAGIC     ) b ON ( a.to_part_sid = b.part_sid )
# MAGIC WHERE
# MAGIC     a.vlt_delete_ind = 'N'
# MAGIC     AND a.vlt_src_tbl_name = 'PLBOM_S'
# MAGIC     AND a.from_part_sid != - 1
# MAGIC     AND a.to_part_sid != - 1
# MAGIC     AND a.from_part_type != 'Parent Sub';

# COMMAND ----------

df=spark.sql("""
select substr(a.ods_table_name, 1, length(a.ods_table_name) -2) || '_S' ods_table_name 
                from ltrefined_proddesign_enovia.ods_type_rel a
                where a.type_rel_ind = 'T'
                    and exists(select 1 from standardized.part z where z.vlt_delete_ind = 'N' and z.vlt_src_tbl_name = substr(a.ods_table_name, 1, length(a.ods_table_name) -2) || '_S')""")

# COMMAND ----------

-- Create table Student with partition
> CREATE TABLE Student (name STRING, rollno INT) PARTITIONED BY (age INT);

> SELECT * FROM Student;
 name rollno age
 ---- ------ ---
  ABC      1  10
  DEF      2  10
  XYZ      3  12

-- Remove all rows from the table in the specified partition
> TRUNCATE TABLE Student partition(age=10);

-- After truncate execution, records belonging to partition age=10 are removed
> SELECT * FROM Student;
 name rollno age
 ---- ------ ---
  XYZ      3  12

-- Remove all rows from the table from all partitions
> TRUNCATE TABLE Student;

> SELECT * FROM Student;
 name rollno age
 ---- ------ ---

# COMMAND ----------

# MAGIC %sql
# MAGIC create table practicetable;