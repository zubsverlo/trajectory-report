# (все функции для запроса таблиц из БД для формирования отчета)
import datetime as dt
from typing import Optional, List, Union

from sqlalchemy import select, Select

from trajectory_report.models import (Statements,
                                      Employees,
                                      Division,
                                      ObjectsSite,
                                      Journal,
                                      Serves,
                                      Coordinates,
                                      Clusters)


def statements(date_from: dt.date,
               date_to: dt.date,
               division: Optional[Union[int, str]] = None,
               name_ids: Optional[List[int]] = None,
               object_ids: Optional[List[int]] = None) -> Select:
    """Получить записи с заявленными выходами"""
    sel = select(
        Statements.name_id,
        Employees.name,
        Statements.object_id,
        ObjectsSite.name.label('object'),
        ObjectsSite.longitude,
        ObjectsSite.latitude,
        Statements.date,
        Statements.statement) \
        .join(Employees) \
        .join(ObjectsSite) \
        .where(Statements.date >= date_from) \
        .where(Statements.date <= date_to).select_from(Statements)
    if isinstance(division, int):
        sel = sel.where(Statements.division == division)
    if isinstance(division, str):
        sel = sel.join(Division, Statements.division == Division.id)
        sel = sel.where(Division.division == division)
    if name_ids:
        sel = sel.where(Statements.name_id.in_(name_ids))
    if object_ids:
        sel = sel.where(Statements.object_id.in_(object_ids))
    return sel


def employee_schedules(name_ids: List[int]) -> Select:
    """Расписание сотрудников"""
    sel: Select = select(
        Employees.name_id,
        Employees.schedule
    ).where(Employees.name_id.in_(name_ids))
    return sel


def journal(name_ids: List[int]) -> Select:
    """Получить записи journal с привязками subscriberID, name_id к датам"""
    sel: Select = select(Journal.name_id,
                         Journal.subscriberID,
                         Journal.period_init,
                         Journal.period_end) \
        .where(Journal.name_id.in_(name_ids))
    return sel


def serves(date_from: dt.date,
           date_to: dt.date,
           name_ids: List[int]) -> Select:
    """Получить служебные записки из БД"""
    sel: Select = select(Serves.name_id,
                         Serves.object_id,
                         Serves.date,
                         Serves.approval,
                         ) \
        .where(Serves.name_id.in_(name_ids)) \
        .where(Serves.date >= date_from) \
        .where(Serves.date <= date_to)
    return sel


def current_locations(subscriber_ids: List[int]) -> Select:
    """get current locations by subscriber_ids"""
    sel: Select = select(Coordinates.subscriberID,
                         Coordinates.locationDate,
                         Coordinates.longitude,
                         Coordinates.latitude) \
        .where(Coordinates.subscriberID.in_(subscriber_ids)) \
        .where(Coordinates.requestDate > dt.date.today()) \
        .where(Coordinates.locationDate != None)
    return sel


def clusters(date_from: dt.date,
             date_to: dt.date,
             subscriber_ids: List[int]) -> Select:
    """Получить кластеры из БД"""
    sel: Select = select(
        Clusters.subscriberID,
        Clusters.date,
        Clusters.datetime,
        Clusters.longitude,
        Clusters.latitude,
        Clusters.leaving_datetime,
        Clusters.cluster) \
        .where(Clusters.subscriberID.in_(subscriber_ids)) \
        .where(Clusters.date >= date_from) \
        .where(Clusters.date < date_to+dt.timedelta(days=1))
    return sel


def statements_one_emp(date: dt.date,
                       name_id: int,
                       division: Union[int, str]
                       ) -> Select:
    sel = select(
        Statements.name_id,
        Employees.name,
        Statements.object_id,
        ObjectsSite.name.label('object'),
        ObjectsSite.longitude,
        ObjectsSite.latitude,
        ObjectsSite.address,
        Statements.date,
        Statements.statement
    ) \
        .join(Employees) \
        .join(ObjectsSite) \
        .where(Statements.date == date) \
        .where(Statements.name_id == name_id).select_from(Statements)
    if isinstance(division, int):
        sel = sel.where(Statements.division == division)
    if isinstance(division, str):
        sel = sel.join(Division, Statements.division == Division.id)
        sel = sel.where(Division.division == division)
    return sel


def locations_one_emp(date: dt.date, subscriber_id: int) -> Select:
    """get locations by subscriber_id and date"""
    sel: Select = select(Coordinates.subscriberID,
                         Coordinates.requestDate,
                         Coordinates.locationDate,
                         Coordinates.longitude,
                         Coordinates.latitude) \
        .where(Coordinates.subscriberID == subscriber_id) \
        .where(Coordinates.requestDate >= date) \
        .where(Coordinates.requestDate < date+dt.timedelta(days=1))
    return sel


def journal_one_emp(name_id: int) -> Select:
    sel: Select = select(Journal.name_id,
                         Journal.subscriberID,
                         Journal.period_init,
                         Journal.period_end) \
        .where(Journal.name_id == name_id)
    return sel
