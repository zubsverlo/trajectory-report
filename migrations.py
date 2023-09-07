from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, String, Index, CHAR, UniqueConstraint, REAL
import datetime as dt
from trajectory_report.config import NEW_DB
from sqlalchemy import create_engine


class Base(DeclarativeBase):
    pass


class Coordinate(Base):
    __tablename__ = 'coordinate'
    id: Mapped[int] = mapped_column(primary_key=True)
    request_dt: Mapped[dt.datetime]
    subscriber: Mapped[int] = mapped_column(nullable=False)
    location_dt: Mapped[dt.datetime]
    lng: Mapped[float] = mapped_column(REAL)
    lat: Mapped[float] = mapped_column(REAL)
    status: Mapped[int]

    __table_args__ = (
        Index('reqDate', 'request_dt'),
    )

    def __repr__(self):
        return f"{self.id}, {self.subscriber}, {self.location_dt}"


class Cluster(Base):
    __tablename__ = 'cluster'
    id: Mapped[int] = mapped_column(primary_key=True)
    subscriber: Mapped[int]
    date: Mapped[dt.date]
    datetime: Mapped[dt.datetime]
    lng: Mapped[float] = mapped_column(REAL)
    lat: Mapped[float] = mapped_column(REAL)
    leaving_dt: Mapped[dt.datetime]
    cluster: Mapped[int] = mapped_column(nullable=False)

    __table_args__ = (
            Index('subsDate', 'subscriber', 'date'),
    )

    def __repr__(self):
        return f"{self.id}, {self.subscriber}, {self.date}"


class Division(Base):
    __tablename__ = 'division'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(CHAR(20))

    def __repr__(self):
        return f"Division({self.id}, {self.name})"


class Employee(Base):
    __tablename__ = "employee"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(CHAR(length=100))
    phone: Mapped[str] = mapped_column(CHAR(length=13))
    address: Mapped[str] = mapped_column(CHAR(length=150))
    hire_date: Mapped[dt.date]
    quit_date: Mapped[dt.date]
    division_id: Mapped[int] = mapped_column(ForeignKey('division.id'))
    bath_attendant: Mapped[bool]
    # division_ref: Mapped['Division'] = relationship('Division', lazy='joined')

    __table_args__ = (
        Index('emp_index', 'id', 'name'),
    )

    def __repr__(self):
        return f"Employees({self.id}, {self.name}, {self.division_id})"


class Object(Base):
    __tablename__ = 'object'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(CHAR(length=100))
    division_id: Mapped[int] = mapped_column(ForeignKey('division.id'))
    address: Mapped[str] = mapped_column(CHAR(length=250))
    phone: Mapped[str] = mapped_column(CHAR(length=200))
    lng: Mapped[float] = mapped_column(REAL)
    lat: Mapped[float] = mapped_column(REAL)
    active: Mapped[bool]
    no_payments: Mapped[bool]
    # division_ref: Mapped['Division'] = relationship('Division', lazy='joined')

    def __repr__(self):
        return f"ObjID: {self.id}, Name: {self.name}"


base = Base()
new_engine = create_engine(NEW_DB)
base.metadata.create_all(new_engine)
