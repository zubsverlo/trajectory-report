# (основной запрос отчетов)
import pandas as pd
from trajectory_report.report import construct_select as cs
from trajectory_report.database import DB_ENGINE
import datetime as dt
from trajectory_report.report.ClusterGenerator import prepare_clusters
from typing import Optional, List, Union
from trajectory_report.exceptions import ReportException


class ReportDataGetter:

    def __init__(self,
                 date_from: dt.date,
                 date_to: dt.date,
                 division: Optional[Union[int, str]] = None,
                 name_ids: Optional[List[int]] = None,
                 object_ids: Optional[List[int]] = None
                 ):
        self._date_from = dt.date.fromisoformat(str(date_from))
        self._date_to = dt.date.fromisoformat(str(date_to))
        (self._stmts,
         self._journal,
         self._schedules,
         self._serves,
         self._clusters) = self._query_data(self._date_from,
                                            self._date_to,
                                            division,
                                            name_ids,
                                            object_ids)

    @staticmethod
    def _query_data(
            date_from: dt.date,
            date_to: dt.date,
            division: Optional[Union[int, str]] = None,
            name_ids: Optional[List[int]] = None,
            object_ids: Optional[List[int]] = None
    ):
        """Формирует select и запрашивает их из БД"""

        includes_current_date: bool = dt.date.today() <= date_to

        """ПОЛУЧЕНИЕ НЕОБХОДИМЫХ ТАБЛИЦ ИЗ БД"""
        with DB_ENGINE.connect() as conn:
            stmts = pd.read_sql(cs.statements(
                date_from=date_from,
                date_to=date_to,
                division=division,
                name_ids=name_ids,
                object_ids=object_ids
            ), conn)
            if not len(stmts):
                raise ReportException(f'No statements found from '
                                 f'{date_from} to {date_to}')
            name_ids = stmts.name_id.unique().tolist()

            journal = pd.read_sql(cs.journal(name_ids), conn)
            journal['period_end'] = journal['period_end'].fillna(
                dt.date.today())
            subs_ids = journal.subscriberID.unique().tolist()

            schedules = pd.read_sql(cs.employee_schedules(name_ids), conn)
            serves = pd.read_sql(cs.serves(date_from, date_to, name_ids), conn)
            clusters = pd.read_sql(cs.clusters(date_from, date_to, subs_ids),
                                   conn)
            if includes_current_date:
                current_locations = pd.read_sql(
                    cs.current_locations(subs_ids),
                    conn
                )
                current_locations['date'] = current_locations['locationDate'] \
                    .apply(lambda x: x.date())

        if includes_current_date:
            try:
                clusters_from_locations = prepare_clusters(current_locations)
                clusters = pd.concat([clusters,
                                      clusters_from_locations])
            except TypeError:
                print("Кластеры по текущим локациям не были сформированы. "
                      "Возможно, из-за недостатка кол-ва локаций.")
        return stmts, journal, schedules, serves, clusters


class OneEmployeeReportDataGetter:

    def __init__(self,
                 name_id: int,
                 date: Union[dt.date, str],
                 division: Union[int, str],
                 ) -> None:
        date = dt.date.fromisoformat(str(date))
        (self._stmts,
         self.clusters,
         self._locations) = self._query_data(name_id, division, date)

    @staticmethod
    def _query_data(name_id: int, division: Union[int, str], date: dt.date):
        with DB_ENGINE.connect() as conn:
            stmts = pd.read_sql(cs.statements_one_emp(date, name_id, division),
                                conn)
            journal = pd.read_sql(cs.journal_one_emp(name_id), conn)
            journal['period_end'] = journal['period_end'].fillna(
                dt.date.today())
            journal = journal \
                .loc[(journal['period_init'] <= date)
                     & (date <= journal['period_end'])]
            try:
                subscriber_id = int(journal.subscriberID.iloc[0])
            except IndexError:
                raise ReportException(f"Employee {name_id} does not have "
                                f"any bounded subscriberID by {date}")

            locations = pd.read_sql(cs.locations_one_emp(date, subscriber_id),
                                    conn)
            valid_locations = locations[pd.notna(locations['locationDate'])]
            if not len(locations) or not len(valid_locations):
                raise ReportException(f"SubscriberID '{subscriber_id}', "
                                f"Name_id {name_id} "
                                f"doesn't have any locations for {date}.")
            clusters = prepare_clusters(valid_locations)
            return stmts, clusters, locations
