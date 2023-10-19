CREATE TABLE
    IF NOT EXISTS "company_info" (
        "cmp_id" VARCHAR (64) NOT NULL,
        "cmp_name" VARCHAR (128) NOT NULL,
        "cmp_scale" VARCHAR (128),
        "cmp_link" VARCHAR (256),
        "cmp_stage" VARCHAR (64),
        "cmp_logo" VARCHAR (256),
        "cmp_industry" VARCHAR (64),
        "cmp_resource" VARCHAR (64),
        PRIMARY KEY ("cmp_id")
    );