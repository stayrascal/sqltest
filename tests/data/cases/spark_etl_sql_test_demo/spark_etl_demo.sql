--variable=target_data_path
--const target_table=top_1_student_by_subject

CREATE TABLE IF NOT EXISTS ${target_table}
(
    subject        STRING,
    student_id     INT,
    student_gender STRING,
    student_age    INT,
    score          INT
)
    USING PARQUET
    LOCATION '${target_data_path}/${target_table}'
;

INSERT OVERWRITE TABLE ${target_table}
SELECT temp.subject,
       temp.student_id,
       s.gender as student_gender,
       s.age    as student_age,
       temp.score
FROM students s
         JOIN
     (SELECT student_id,
             subject,
             score,
             dense_rank() OVER (PARTITION BY subject ORDER BY score DESC) as rank
      FROM subject_scores) temp ON s.id = temp.student_id
WHERE temp.rank = 1
;