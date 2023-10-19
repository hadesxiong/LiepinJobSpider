CREATE TABLE
    IF NOT EXISTS "recruiter_info" (
        "rec_id" VARCHAR(128) NOT NULL,
        "rec_imid" VARCHAR(128) NOT NULL,
        "rec_imtype" VARCHAR(32) NOT NULL,
        "rec_name" VARCHAR(64) NOT NULL,
        "rec_title" VARCHAR(128),
        "rec_img" VARCHAR(256),
        "rec_resource" VARCHAR(256),
        PRIMARY KEY ("rec_id")
    );