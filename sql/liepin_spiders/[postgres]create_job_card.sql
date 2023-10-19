CREATE TABLE
    IF NOT EXISTS "job_card" (
        "job_id" VARCHAR(128) NOT NULL,
        "job_name" VARCHAR(128) NOT NULL,
        "job_refer" VARCHAR(255),
        "job_salary" VARCHAR(64),
        "job_edu" VARCHAR(128) NOT NULL,
        "job_dq" VARCHAR(128),
        "job_link" VARCHAR(256),
        "job_kind" VARCHAR(32),
        "job_years" VARCHAR(64),
        "job_label" VARCHAR(32) [] NULL,
        "job_refreshtime" VARCHAR(32),
        "job_resource" VARCHAR(32),
        "cmp_id" VARCHAR(32) NULL,
        "rec_id" VARCHAR(128) NULL,
        PRIMARY KEY ("job_id")
    )