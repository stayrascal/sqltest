--variable=env
--variable=target_data_path
--const target_table=dw_dim_${env}.student_info

CREATE TABLE IF NOT EXISTS ${target_table}
(
    student_id     INT,
    student_age    INT,
    student_gender STRING
)
    USING PARQUET
    LOCATION '${target_data_path}/${target_table}'
;

CREATE TABLE IF NOT EXISTS ${target_table}_temp
(
    student_id     INT,
    student_age    INT,
    student_gender STRING
)
    USING PARQUET
    LOCATION '${target_data_path}/${target_table}_temp'
;

INSERT OVERWRITE TABLE ${target_table}_temp
SELECT
       id AS student_id,
       age as student_age,
       gender AS student_gender
FROM dw_dim_${env}.students s
LEFT JOIN ${target_table} tt ON s.id = tt.student_id
WHERE tt.student_id IS NULL
;

INSERT OVERWRITE TABLE ${target_table}
SELECT
       student_id,
       student_age,
       student_gender
FROM ${target_table}_temp
;

DROP TABLE ${target_table}_temp;
