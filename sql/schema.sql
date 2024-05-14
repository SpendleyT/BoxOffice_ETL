-- Drop tables if there to rebuild db
DROP TABLE IF EXISTS box_office;
DROP TABLE IF EXISTS movie;
DROP TABLE IF EXISTS distributor;

-- Create dimension table distributor
CREATE TABLE distributor (
    distributor_id INTEGER GENERATED ALWAYS AS IDENTITY,
    distributor_name VARCHAR(50) UNIQUE NOT NULL,
    PRIMARY KEY(distributor_id)
);

-- Create dimension table movie
CREATE TABLE movie (
    movie_id INTEGER GENERATED ALWAYS AS IDENTITY,
    distributor_id INTEGER,
    title VARCHAR(200) NOT NULL,
    release_year INTEGER NOT NULL,
    runtime INTEGER,
    poster_url VARCHAR(200),
    box_office_gross BIGINT,
    meta_score DECIMAL(4, 1),
    rt_score DECIMAL(4,1),
    imdb_score DECIMAL(4, 1),
    imdb_ref_id VARCHAR(12),
    genre VARCHAR(200),
    PRIMARY KEY(movie_id),
    CONSTRAINT fk_distributor
        FOREIGN KEY (distributor_id) REFERENCES distributor(distributor_id)
);

-- Create fact table box_office
CREATE TABLE box_office (
    box_office_id INTEGER GENERATED ALWAYS AS IDENTITY,
    movie_id INTEGER,
    bo_year INTEGER NOT NULL,
    bo_week INTEGER NOT NULL,
    bo_rank INTEGER NOT NULL,
    weekly_gross DECIMAL(12, 0),
    theatres INTEGER,
    per_theatre DECIMAL (7, 0),
    release_week INTEGER,
    PRIMARY KEY(box_office_id),
    CONSTRAINT fk_movie
        FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
);