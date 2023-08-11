# (основной запрос отчетов)
import pandas as pd
from trajectory_report.report import construct_select as cs
from trajectory_report.database import DB_ENGINE
import datetime as dt
from trajectory_report.report.ClusterGenerator import prepare_clusters
from typing import Optional, List, Union, Any
from trajectory_report.exceptions import ReportException
import redis
import pickle
import bz2


class CachedReportDataGetter:
    CACHED_SELECTS = {
        'statements': cs.statements_only,
        'employees': cs.employees,
        'schedules': cs.employee_schedules,
        'objects': cs.objects,
        'journal': cs.journal,
        'serves': cs.serves,
        'clusters': cs.clusters,
        'divisions': cs.divisions,
        'current_locations': cs.current_locations
    }

    def get_data(self,
                 date_from: Union[dt.date, str],
                 date_to: Union[dt.date, str],
                 division: Optional[Union[int, str]] = None,
                 name_ids: Optional[List[int]] = None,
                 object_ids: Optional[List[int]] = None
                 ) -> dict:
        self._date_from = dt.date.fromisoformat(str(date_from))
        self._date_to = dt.date.fromisoformat(str(date_to))
        includes_current_date: bool = dt.date.today() <= self._date_to
        self.__r_conn = redis.Redis()
        self.__cache_date_from = \
            ((dt.date.today().replace(day=1) - dt.timedelta(days=1))
             .replace(day=1))

        stmts = self.__get_statements(division, name_ids, object_ids)
        self.__name_ids = stmts.name_id.unique().tolist()

        journal = self.__get_cached_or_updated('journal')
        journal = self.__filter_by_column(journal, ['name_id'])
        journal.loc['period_end'] = journal['period_end'].fillna(
            dt.date.today())
        self.__subs_ids = journal.subscriberID.unique().tolist()

        schedules = self.__get_cached_or_updated('schedules')
        schedules = self.__filter_by_column(schedules, ['name_id'])

        serves = self.__get_cached_or_updated('serves')
        serves = self.__filter_by_column(serves,
                                               ['name_id', 'date_from',
                                                'date_to'])

        clusters = self.__get_cached_or_updated('clusters')
        clusters = self.__filter_by_column(clusters,
                                           ['subscriberID', 'date_from',
                                            'date_to'])

        if includes_current_date:
            current_locs = self.__get_cached_or_updated('current_locations')
            current_locs = self.__filter_by_column(current_locs,
                                                   'subscriberID')
            current_locs['date'] = (current_locs['locationDate']
                                    .apply(lambda x: x.date()))
            try:
                clusters = pd.concat([clusters,
                                            prepare_clusters(current_locs)])
            except (TypeError, AttributeError):
                print("Кластеры по текущим локациям не были сформированы. "
                      "Возможно, из-за недостатка кол-ва локаций.")

        data = dict()
        data['_stmts'] = stmts
        data['_journal'] = journal
        data['_schedules'] = schedules
        data['_serves'] = serves
        data['_clusters'] = clusters
        return data

    def __get_statements(self,
                         division: Optional[Union[int, str]] = None,
                         name_ids: Optional[List[int]] = None,
                         object_ids: Optional[List[int]] = None
                         ) -> pd.DataFrame:
        statements = self.__get_cached_or_updated('statements')
        divisions = self.__get_cached_or_updated('divisions')
        statements = pd.merge(statements, divisions, on='division')
        if isinstance(division, str):
            statements = statements[statements['division_name'] == division]
        if isinstance(division, int):
            statements = statements[statements['division'] == division]

        if name_ids:
            statements = statements[statements['name_id'].isin(name_ids)]
        if object_ids:
            statements = statements[statements['name_id'].isin(object_ids)]

        statements = statements[(statements['date'] >= self._date_from) &
                                (statements['date'] <= self._date_to)]

        objects = self.__get_cached_or_updated('objects')
        employees = self.__get_cached_or_updated('employees')

        statements = pd.merge(statements, objects, on=['object_id'])
        statements = pd.merge(statements, employees, on=['name_id'])
        return statements[['name_id', 'object_id', 'name', 'object',
                           'longitude', 'latitude', 'date', 'statement',
                           'division']]

    def __filter_by_column(self, obj: pd.DataFrame,
                           filter: List[str]):
        """Фильтр для собранного из кеша dataframe с указанием полей для
        заполнения и фильтрации"""
        if 'name_id' in filter:
            obj = obj[obj['name_id'].isin(self.__name_ids)]
        if 'subscriberID' in filter:
            obj = obj[obj['subscriberID'].isin(self.__subs_ids)]
        if 'date_from' in filter:
            obj = obj[obj['date'] >= self._date_from]
        if 'date_to' in filter:
            obj = obj[obj['date'] <= self._date_to]

        return obj

    def __get_cached_or_updated(self, key):
        res = self.__get_from_redis(key)
        if res is None:
            with DB_ENGINE.connect() as conn:
                res = pd.read_sql(
                    CachedReportDataGetter.CACHED_SELECTS[key](
                        date_from=self.__cache_date_from),
                    conn
                )
            self.__send_to_redis(key, res)
        return res

    def __get_from_redis(self, key: str) -> Any:
        """"Fetch from redis by key, decompress and unpickle"""
        fetched = self.__r_conn.get(key)
        if not fetched:
            return None
        return pickle.loads(bz2.decompress(fetched))

    def __send_to_redis(self, key: str, obj: Any) -> bool:
        """Compress, pickle and set as a key"""
        self.__r_conn.set(key, bz2.compress(pickle.dumps(obj)))
        return True


class DatabaseReportDataGetter:

    @staticmethod
    def get_data(
            date_from: Union[dt.date, str],
            date_to: Union[dt.date, str],
            division: Optional[Union[int, str]] = None,
            name_ids: Optional[List[int]] = None,
            object_ids: Optional[List[int]] = None
    ) -> dict:
        """Формирует select и запрашивает их из БД"""
        date_from = dt.date.fromisoformat(str(date_from))
        date_to = dt.date.fromisoformat(str(date_to))
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
                raise ReportException(f'Не найдено заявленных выходов в период '
                                 f'с {date_from} до {date_to}')
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
            except (TypeError, AttributeError):
                print("Кластеры по текущим локациям не были сформированы. "
                      "Возможно, из-за недостатка кол-ва локаций.")
        data = dict()
        data['_stmts'] = stmts
        data['_journal'] = journal
        data['_schedules'] = schedules
        data['_serves'] = serves
        data['_clusters'] = clusters
        return data


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
                raise ReportException(
                    f"За сотрудником не закреплено ни одного "
                    f"устройства в этот день ({date})")

            locations = pd.read_sql(cs.locations_one_emp(date, subscriber_id),
                                    conn)
            valid_locations = locations[pd.notna(locations['locationDate'])]
            if not len(locations) or not len(valid_locations):
                raise ReportException(f"По данному сотруднику не обнаружено "
                                      f"локаций за {date}.")
            clusters = prepare_clusters(valid_locations)
            return stmts, clusters, locations


def report_data_factory(date_from: Union[dt.date, str], *args, use_cache=True,
                        **kwargs
                        ) -> dict:
    try:
        r = redis.Redis()
        redis_available = r.ping()
    except redis.ConnectionError:
        redis_available = False

    date_from = dt.date.fromisoformat(str(date_from))
    cache_date_from = \
       ((dt.date.today().replace(day=1)) - dt.timedelta(days=1)).replace(day=1)
    if date_from >= cache_date_from and redis_available and use_cache:
        data = CachedReportDataGetter().get_data(date_from, *args, **kwargs)
    else:
        data = DatabaseReportDataGetter().get_data(date_from, *args, **kwargs)
    return data



