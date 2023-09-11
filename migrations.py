from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, String, Index, CHAR, UniqueConstraint, REAL
import datetime as dt
from trajectory_report.config import NEW_DB
from sqlalchemy import create_engine, text


class Base(DeclarativeBase):
    pass


class Coordinate(Base):
    __tablename__ = 'coordinate'
    id: Mapped[int] = mapped_column(primary_key=True)
    request_dt: Mapped[dt.datetime]
    subscriber: Mapped[int] = mapped_column(nullable=False)
    location_dt: Mapped[dt.datetime] = mapped_column(nullable=True)
    lng: Mapped[float] = mapped_column(REAL, nullable=True)
    lat: Mapped[float] = mapped_column(REAL, nullable=True)
    status: Mapped[int] = mapped_column(nullable=True)

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
            Index('clusterSubsDate', 'subscriber', 'date'),
            Index('clusterDate', 'date')
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


class Journal(Base):
    __tablename__ = 'journal'
    id: Mapped[int] = mapped_column(primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey('employee.id'))
    subscriber: Mapped[int]
    period_init: Mapped[dt.date]
    period_end: Mapped[dt.date]
    # name: Mapped['Employees'] = relationship('Employee', lazy='joined')
    __table_args__ = (
        Index('journalEmployeeIndex', 'employee_id'),
    )

    def __repr__(self):
        return (f"Subs: {self.subscriber} Name_id: {self.employee_id} "
                f"Init: {self.period_init} End: {self.period_end}")


class Serve(Base):
    __tablename__ = 'serve'
    id: Mapped[int] = mapped_column(primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey('employee.id'))
    object_id: Mapped[int] = mapped_column(ForeignKey('object.id'))
    date: Mapped[dt.date]
    comment: Mapped[str] = mapped_column(String(200))
    address: Mapped[str] = mapped_column(String(255))
    approval: Mapped[int]
    # employee: Mapped['Employees'] = relationship('Employees', lazy='joined')
    # object: Mapped['ObjectsSite'] = relationship('ObjectsSite', lazy='joined')
    __table_args__ = (
        Index('serveDateIndex', 'date'),
        Index('employeeDateIndex' 'date', 'employee_id')
    )

    def __repr__(self):
        return (f"Name: {self.employee_id} Object: {self.object_id} "
                f"Date: {self.date} Approval: {self.approval}")


class Statement(Base):
    __tablename__ = "statement"
    id: Mapped[int] = mapped_column(primary_key=True)
    division_id: Mapped[int] = mapped_column(ForeignKey('division.id'))
    employee_id: Mapped[int] = mapped_column(ForeignKey('employee.id'))
    object_id: Mapped[int] = mapped_column(ForeignKey('object.id'))
    date: Mapped[dt.date]
    statement: Mapped[str] = mapped_column(CHAR(length=1))
    __table_args__ = (
        Index('statementDivisionDateIndex', 'division_id', 'date'),
        UniqueConstraint('division_id', 'object_id', 'date', 'employee_id',
                         name='_statements_unique'),
    )

    def __repr__(self):
        return (f"Statements({self.id}, {self.division_id}, {self.employee_id}, "
                f"{self.object_id}, {self.date}, {self.statement})")


base = Base()
new_engine = create_engine(NEW_DB)
base.metadata.create_all(new_engine)

conn = new_engine.connect()
# conn.execute(text("""insert into gpsdev.coordinate(id, request_dt, subscriber, location_dt, lng, lat) select locationID, requestDate, subscriberID, locationDate, longitude, latitude from gps.coordinates where requestDate > "2023-09-08" """))
conn.commit()
# conn.execute(text("""insert into gpsdev.coordinate(id, request_dt, subscriber, location_dt, lng, lat) select locationID, requestDate, subscriberID, locationDate, longitude, latitude from gps.coordinates where requestDate > "2023-09-08" """))
conn.close()
