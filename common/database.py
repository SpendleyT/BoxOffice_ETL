from sqlalchemy import create_engine, func, select, update, delete, text
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey
from sqlalchemy.orm import Session, declarative_base
import psycopg2
from pandas import Series
import os

#Define ORM data class base
Base = declarative_base()

#Define database configurations
DB_NAME = 'box_office_db'
DB_ADDRESS = 'localhost:5432'
DB_USER = 'postgres'
DB_PASSWORD = os.environ['DB_PASSWORD_KEY']


class DatabaseConnection:
    def __init__(self):
        self._engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_ADDRESS}/{DB_NAME}b",
            isolation_level = "REPEATABLE READ"
        )
        self._session = Session(self._engine)


    @property
    def connection(self):
        return self._conn


    def close(self):
        self._session.close_all()
        self._engine.dispose()
        return True


    def add_box_office_entry(self, week_info: Series, movie_id: int):
        new_box_office = BoxOffice(
            movie_id=movie_id,
            bo_year=int(week_info['Reference'].split('-')[0]),
            bo_week=int(week_info['Reference'].split('-')[1]),
            bo_rank=int(week_info['Rank']),
            weekly_gross = int(week_info['Gross']),
            theatres=int(week_info['Theaters']),
            per_theatre=int(week_info['Average']),
            release_week=int(week_info['Weeks'])
        )
        self._session.add(new_box_office)
        self._session.flush()
        id = new_box_office.box_office_id
        self._session.commit()
        return id


    def add_movie(self, movie_info: Series, dist_id: int):
        new_movie = Movie(
            distributor_id=dist_id,
            title=movie_info['Release'],
            release_year=movie_info['Year'],
            runtime=int(movie_info['Runtime'].split(' ')[0]),
            poster_url=movie_info['Poster'],
            box_office_gross=movie_info['Total Gross'],
            meta_score=movie_info['meta_score'],
            rt_score=movie_info['rt_score'],
            imdb_score=movie_info['imdb_score'],
            imdb_ref_id=movie_info['imdbID'],
            genre=movie_info['Genre']
        )
        self._session.add(new_movie)
        self._session.flush()
        id = new_movie.movie_id
        self._session.commit()
        return id


    def add_distributor(self, distributor: str) -> int:
        new_distributor = Distributor(distributor_name=distributor)
        self._session.add(new_distributor)
        self._session.flush()
        id = new_distributor.distributor_id
        self._session.commit()
        return id


    def get_movie_id_by_name(self, title: str):
        stmt = select(Movie).where(Movie.title == title)
        result = self._session.execute(stmt).fetchone()
        print(f"mov-by-nm result = {result}")
        return 0 if not result else result[0].movie_id


    def get_distributor_id_by_name(self, name: str):
        stmt = select(Distributor).where(Distributor.distributor_name == name)
        result = self._session.execute(stmt).fetchone()
        print(f"dist-by-nm result = {result}")
        return 0 if not result else result[0].distributor_id



class Distributor(Base):
    __tablename__ = "distributor"
    distributor_id = Column(Integer, primary_key=True)
    distributor_name = Column(String)


class Movie(Base):
    __tablename__ = "movie"
    movie_id = Column(Integer, primary_key=True)
    distributor_id = Column(
        Integer, 
        ForeignKey('distributor.distributor_id', ondelete='CASCADE')
    )
    title = Column(String)
    release_year = Column(Integer)
    runtime = Column(Integer)
    poster_url = Column(String)
    box_office_gross = Column(Integer)
    meta_score = Column(Float)
    rt_score = Column(Float)
    imdb_score = Column(Float)
    imdb_ref_id = Column(String)
    genre = Column(String)


class BoxOffice(Base):
    __tablename__  = 'box_office'
    box_office_id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movie.movie_id', ondelete='CASCADE'))
    bo_year = Column(Integer)
    bo_week = Column(Integer)
    bo_rank = Column(Integer)
    weekly_gross = Column(Integer)
    theatres = Column(Integer)
    per_theatre = Column(Integer)
    release_week = Column(Integer)
