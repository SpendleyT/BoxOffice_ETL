SELECT box_office.box_office_id, box_office.bo_year, box_office.bo_week, box_office.bo_rank,
	box_office.weekly_gross, box_office.theatres, movie.title, movie.runtime, 
	movie.meta_score, movie.rt_score, movie.imdb_score, movie.genre, 
	distributor.distributor_name
FROM box_office 
	LEFT JOIN movie ON movie.movie_id = box_office.movie_id
	LEFT JOIN distributor ON distributor.distributor_id = movie.distributor_id
ORDER BY bo_year, bo_week, bo_rank;


TRUNCATE TABLE box_office;
TRUNCATE TABLE movie CASCADE;
TRUNCATE TABLE distributor CASCADE;

