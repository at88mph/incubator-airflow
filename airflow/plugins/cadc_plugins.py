import logging

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.redis_hook import RedisHook
from urllib import parse as parse


# class ExtractFileNamesOperator(KubernetesPodOperator):
#     """
#     Pull filenames for a Collection.  Uses Kubernetes to write to a volume.
#     """

#     def __init__(self, collection, http_conn_id, redis_conn_id, output_format='csv', *args, **kwargs):
#         super(ExtractFileNamesOperator, self).__init__(
#             image='appropriate/curl', *args, **kwargs)
#         self.collection = collection
#         self.http_conn_id = http_conn_id
#         self.redis_conn_id = redis_conn_id
#         self.output_format = output_format
#         self.datetime_format = '%Y-%m-%d %H:%M:%S'

#     def _to_millisecons(self, dt):
#         return int(round(dt.timestamp() * 1000))

#     def execute(self, context):
#         http_conn = BaseHook.get_connection(self.http_conn_id)
#         prev_exec_date = context.get('prev_execution_date')
#         next_exec_date = context.get('next_execution_date')
#         query_meta = "SELECT fileName FROM archive_files WHERE archiveName = '{}'" \
#                      " AND ingestDate > '{}' and ingestDate <= '{}' ORDER BY ingestDate".format(self.collection.upper(),
#                                                                                                 prev_exec_date.strftime(
#                                                                                                     self.datetime_format),
#                                                                                                 next_exec_date.strftime(self.datetime_format))
#         logging.info('Query: {}'.format(query_meta))
#         data = {'QUERY': query_meta, 'LANG': 'ADQL',
#                 'FORMAT': '{}'.format(self.output_format)}

#         self.arguments = ['-sSL', '-u', '{}:{}'.format(http_conn.login, http_conn.password), '-o',
#                           '/data/cadc/{}/files_{}_{}.{}'.format(self.collection,
#                                                                 self._to_millisecons(
#                                                                     prev_exec_date),
#                                                                 self._to_millisecons(next_exec_date), self.output_format),
#                           '{}/ad/auth-sync?{}'.format(http_conn.host, parse.urlencode(data))]

#         super(ExtractFileNamesOperator, self).execute(context)


class TransformFileNamesOperator(KubernetesPodOperator):
    """
    Read filenames and transform them to CAOM-2 entries.
    """

    def __init__(self, collection, http_conn_id, redis_conn_id, output_format='csv', *args, **kwargs):
        super(TransformFileNamesOperator, self).__init__(
            image='opencadc/vlass2caom2:batch', *args, **kwargs)
        self.collection = collection.upper()
        self.http_conn_id = http_conn_id
        self.redis_conn_id = redis_conn_id
        self.output_format = output_format

    def _to_millisecons(self, dt):
        return int(round(dt.timestamp() * 1000))

    def execute(self, context):
        auth_conn = HttpHook.get_connection(self.http_conn_id)
        http_conn = HttpHook('GET', self.http_conn_id)
        redis_conn = RedisHook(self.redis_conn_id)

        with http_conn.run('/cred/auth/priv/users/{}'.format(auth_conn.login)) as response:
            cert = response.text
            prev_date = self._to_millisecons(context.get('prev_execution_date'))
            next_date = self._to_millisecons(context.get('next_execution_date'))
            input_file_names_key = '{}_{}_{}.{}'.format(
                self.collection, prev_date, next_date, self.output_format)
            input_file_names = redis_conn.get_conn().get(input_file_names_key).decode('utf-8')
            input_file_name_strings = ' '.join(str(x) for x in input_file_names.split()[1:])
            input_file_name_array = input_file_name_strings.split()[1:]
            input_file_name_array.append('{}'.format(cert))

            self.cmds = ['vlass_run_batch.sh']
            self.arguments = input_file_name_array

            super(TransformFileNamesOperator, self).execute(context)
