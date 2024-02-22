CREATE TABLE worker(
    id IDENTITY PRIMARY KEY,
    name VARCHAR(1000) NOT NULL CHECK (CHAR_LENGTH(name) >= 2),
    birthday DATE,
    level VARCHAR(7) NOT NULL,
    salary INT);

ALTER TABLE worker
ADD CONSTRAINT level_enum_values
CHECK (level IN('Trainee', 'Junior', 'Middle', 'Senior'));

ALTER TABLE worker
ADD CONSTRAINT salary_limits
CHECK (salary >= 100 AND salary <= 100000);

ALTER TABLE worker
ADD CONSTRAINT birthday_limit
CHECK (EXTRACT(YEAR FROM birthday) > 1900);

CREATE TABLE client(
    id IDENTITY PRIMARY KEY,
    name VARCHAR(1000) NOT NULL CHECK(CHAR_LENGTH(name) >=2));

CREATE TABLE project(
    id IDENTITY PRIMARY KEY,
    client_id BIGINT,
    start_date DATE,
    finish_date DATE);

CREATE TABLE project_worker(
    project_id BIGINT NOT NULL,
    worker_id   BIGINT NOT NULL,
    PRIMARY KEY(project_id, worker_id),
    FOREIGN KEY(project_id) REFERENCES project(id),
    FOREIGN KEY(worker_id) REFERENCES worker(id));