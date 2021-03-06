import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Text, DateTime, Integer, ForeignKey, MetaData, ForeignKeyConstraint
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy_utils import database_exists, create_database


def create_db():
    # create database
    db_string = 'postgres+psycopg2://dhyungseoklee:stackoverflowpw@localhost/stack_overflow'
    engine = create_engine(db_string)
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine


def create_table(engine):
    Base.metadata.create_all(engine, checkfirst = True)


# create data model
Base = declarative_base()


class Users(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key = True)
    display_name = Column('display_name', Text())
    about_me = Column('about_me', Text())
    age = Column('age', Text())
    creation_date = Column('creation_date', DateTime)
    last_access_date = Column('last_access_date', DateTime)
    location = Column('location', Text())
    reputation = Column('reputation', Integer)
    up_votes = Column('up_votes', Integer)  
    down_votes = Column('down_votes', Integer)  
    views = Column('views', Integer)
    profile_image_url = Column('profile_image_url', Text())
    website_url = Column('website_url', Text())
    # one to many with posts
    owner_posts = relationship('Posts', back_populates = 'owner_user', foreign_keys = [id])
    # one to many with post_answers
    owner_posts_answer = relationship('Post_answers', back_populates = 'owner_user_answers', foreign_keys = [id]) 

class Posts(Base):
    __tablename__ = 'posts'
    
    id = Column(Integer, primary_key = True)
    title = Column('title', Text())
    body = Column('body', Text())
    accepted_answer_id = Column('accepted_answer_id', Integer, ForeignKey('posts_answers.id'))
    answer_count = Column('answer_count', Integer)
    comment_count = Column('comment_count', Integer)
    community_owned_date = Column('community_owned_date', DateTime)
    creation_date = Column('creation_date', DateTime)
    favorite_count = Column('favorite_count', Integer)
    owner_display_name = Column('owner_display_name', Text())
    owner_user_id = Column('owner_user_id', Integer, ForeignKey('users.id'))
    parent_id = Column('parent_id', Integer)
    post_type_id = Column('post_type_id', Integer)
    score = Column('score', Integer)
    tags = Column('tags', Text())
    view_count = Column('view_count', Integer)
    # many to one relationship
    owner_user = relationship('Users', back_populates = 'owner_posts', foreign_keys = [owner_user_id])
    # one to one with post_answers
    answer = relationship('Post_answers', uselist = False, back_populates = 'post_answers', foreign_keys = [accepted_answer_id])



class Post_answers(Base):
    __tablename__ = 'posts_answers'

    id = Column(Integer, primary_key = True)
    title = Column('title', Text())
    body = Column('body', Text())
    accepted_answer_id = Column('accepted_answer_id', Integer)
    answer_count = Column('answer_count', Integer)
    comment_count = Column('comment_count', Integer)
    community_owned_date = Column('community_owned_date', DateTime)
    creation_date = Column('creation_date', DateTime)
    favorite_count = Column('favorite_count', Integer)
    owner_display_name = Column('owner_display_name', Text())
    owner_user_id = Column('owner_user_id', Integer, ForeignKey('users.id'))
    parent_id = Column('parent_id', Integer)
    post_type_id = Column('post_type_id', Integer)
    score = Column('score', Integer)
    tags = Column('tags', Text())
    view_count = Column('view_count', Integer)
    # one to one with post
    post_answers = relationship('Posts', back_populates = 'answer', foreign_keys = [id])
    # many to one with user
    owner_user_answers = relationship('Users', back_populates = 'owner_posts_answer', foreign_keys = [owner_user_id])







    
