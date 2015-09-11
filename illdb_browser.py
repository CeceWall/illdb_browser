#!/usr/bin/env python

import argparse
from collections import defaultdict
import json
import logging
import sys
import binascii
import MySQLdb
import MySQLdb.cursors
import zlib
from _mysql_exceptions import *

logging.basicConfig()
logger = logging.getLogger('illdb_browser')


class Handler(object):
    COMMON_ILLDB_COLUMNS = ['_id', 'docid', 'document', 'created', 'modified', 'cas', 'expired', 'meta']

    def __init__(self, **kwargs):
        self.connections = []
        for name, value in kwargs.items():
            setattr(self, name, value)
        try:
            self.manage_connection = self.__get_connection(self.host, self.port, self.username, self.password,
                                                           self.bucket)
        except:
            logger.error("couldn't connect to bucket {},please check your database arguments.".format(self.bucket))
            self.error_exit()
        """
        use a nested dict to store manage configs
        here's a sample
        {
            "configs":
                {
                    "db_prefix":{"name": "db_prefix","value": "janus"},
                    "last_node":{"name": "last_node","value": "1"},
                }
        }
        """
        self.manage_configs = self.__get_database_configs(self.manage_connection)

    def error_exit(self):

        sys.exit(-1)

    def __get_database_configs(self, connection):
        """
        this method fetch management db data and store it into field illdb_info
        :param connection: mysql connection to destination database
        :return a nested dict contains config data
        """
        fields = {
            'configs': (('name',), ('value',)),
            'nodes': (("id",), ("host", "port", "username", "pwd", "status", "weight",)),
            'namespaces': (('name',), ('numshards', 'created',)),
        }
        configs = defaultdict(dict)
        try:
            for name, table in fields.items():
                primary_keys_name, keys_name = table
                columns = primary_keys_name + keys_name
                res = self.__query(connection=connection, table=name, columns=columns,
                                   cursorclass=MySQLdb.cursors.DictCursor)
                for row in res:
                    item = {}
                    for column in columns:
                        item[column] = row[column]
                    if len(primary_keys_name) > 1:
                        primary_keys = tuple()
                        for primary_key in table[0]:
                            primary_keys += (row[primary_key],)
                    else:
                        primary_keys = row[primary_keys_name[0]]
                    configs[name][primary_keys] = item
        except OperationalError as e:
            logger.error(
                "Couldn't fetch illdb configs from database {},pleas check your bucket name.".format(self.bucket))
            self.error_exit()
        return configs

    def __query(self, connection, table, columns, args=None, limit=None, one_row=False,
                cursorclass=MySQLdb.cursors.Cursor):
        """
        query database and return result sets

        :param connection: mysql connection to destination database
        :param table: name of table
        :param columns: a list stored selected columns
        :param args: a list stored tuples of each argument, a tuple was consist of key,operator,value.Here's a sample
                    ('name',MySQLOperator.eq,'db_prefix')
        :param limit: max rows of this sql fetch
        :param cursorclass: cursorclass if you need a special Cursor
        :param single_row: indicate if query only return one row
        :return: a iterable object of all query result rows, structure of one row was depended on cursorclass
        """
        sql = "select {} from {} where 1=1 "
        sql = sql.format(','.join(columns), table)
        args_mapping = None
        if args:
            args_mapping = {}
            for arg in args:
                name, operator, value = arg
                sql += ' and {} {} %({})s'.format(name, operator, name)
                args_mapping[name] = value
        if limit is not None:
            sql += " limit %d" % limit
        cursor = connection.cursor(cursorclass)
        try:
            cursor.execute(sql, args=args_mapping)
        except InterfaceError:
            logger.error('database connection was broken, please try again')
            self.error_exit()
        if one_row:
            res = cursor.fetchone()
        else:
            res = cursor.fetchall()
        cursor.close()
        return res

    def __get_connection(self, host, port, username, password, database, timeout=20):
        """
        build a connection to indicated database
        :param host:
        :param port:
        :param username:
        :param password:
        :param database:
        :param timeout:
        :return: a MySQLdb connection instance
        """
        db = MySQLdb.connect(host=host, port=port, user=username, passwd=password, db=database, connect_timeout=timeout)
        self.connections.append(db)
        return db

    def __close_connection(self, connection):
        if connection in self.connections:
            self.connections.remove(connection)
            connection.close()

    def __split_docid(self, docid):
        d = docid.split(":")
        if len(d) < 3:
            logger.error("docid is invalid, it should be consist of three part at least.")
            self.error_exit()
        return d[:3]

    def __get_namespace(self, docid):
        """
        this method return a dict which store namespace indicated by docid
        :param docid:a valid docid contains namespace name
        :return:a dict contains namespace info
        """
        namespace_name, doc_type, key = self.__split_docid(docid)
        # set namespace_name to global if namespace_name is wildcard or target bucket is game bucket
        namespace_name = 'global' if namespace_name == '*' or self.bucket == "game" else namespace_name
        namespace = self.manage_configs['namespaces'].get(namespace_name)
        if namespace is None:
            message = "Couldn't found namespace {}, docid maybe invalid".format(namespace_name)
            logger.error(message)
            self.error_exit()
        return namespace

    def __get_doc_type(self, docid):
        namespace, doc_type, key = self.__split_docid(docid)
        return doc_type

    def __get_shard_index(self, docid):
        namespace = self.__get_namespace(docid)
        numshards = namespace['numshards']
        return binascii.crc32(docid) % numshards

    def __get_shard_node(self, namespace, shard_index):
        """
        this method get node info base on namespace and shard_index
        :param namespace: namespace of the shard
        :param shard_index: index of the shard
        :return: a tuple consist of node id and node status
        """
        table = "shards"
        columns = ('id', 'namespace', 'node', 'status')
        args = [
            ('id', '=', shard_index),
            ('namespace', '=', namespace['name']),
        ]

        res = self.__query(connection=self.manage_connection, table=table, columns=columns, args=args, one_row=True,
                           cursorclass=MySQLdb.cursors.DictCursor)
        return (res['node'], res['status'])

    def __compress_document(self, document):
        return zlib.compress(document)

    def __decompress_document(self, document):
        """
        this method would decompress document if it was compressed
        :param document: decompressed document,if document wasn't compressed, an exception would be raised.
        :return:decompressed document
        """
        return zlib.decompress(document)

    def handle(self):
        if self.command == "get":
            self.do_get(self.docid)
        elif self.command == 'set':
            self.do_set(self.docid, self.document)

    def __build_node_connection(self, docid):
        """
        this method build a connection a node server
        :param docid: docid
        :return: a tuple consist of database name,table name and node id
        """
        db_prefix = self.manage_configs['configs']['db_prefix']['value']
        namespace = self.__get_namespace(docid)
        shard_index = self.__get_shard_index(docid)
        database = "{}_{}_{}_data".format(db_prefix, namespace['name'], shard_index)

        node_id, node_status = self.__get_shard_node(namespace, shard_index)
        if node_status != 'active':
            message = "Could't fetch document from an inactive node, node id {} ".format(node_id)
            logger.error(message)
            self.error_exit()
        node = self.manage_configs["nodes"][node_id]
        try:
            node_connection = self.__get_connection(node['host'], node['port'], node['username'], node['pwd'], database)
        except StandardError as e:
            logger.error('could not build connection to node database,please confirm your docid is vaild')
            self.error_exit()
        return node_connection

    def __get_document(self, docid, node_connection):
        doc_type = self.__get_doc_type(docid)
        table = "data_{}".format(doc_type)
        args = [('docid', '=', docid)]
        res = self.__query(node_connection, table, Handler.COMMON_ILLDB_COLUMNS, args, one_row=True,
                           cursorclass=MySQLdb.cursors.DictCursor)
        if res is None:
            message = 'document not found,please check your docid'
            logger.error(message)
            self.error_exit()
        else:
            return res

    def __modify_size(self, document):
        """
        this method would modify document size to correct value
        :param document: document you want to modify
        :return: modified document and size
        """
        try:
            doc_object = json.loads(document)
            doc_object['_meta']['size'] = 0
        except ValueError:
            logger.error("document is not a valid json, please check it.")
            self.error_exit()
        except KeyError:
            logger.error("document do not have _meta field, and it's a invalid document.")
            self.error_exit()
        doc_json = json.dumps(doc_object, separators=(',', ':'))
        size = len(doc_json) - 1
        size += len(str(size))
        doc_object['_meta']['size'] = size
        return json.dumps(doc_object, separators=(',', ':')), size

    def do_get(self, docid):
        node_connection = self.__build_node_connection(docid)
        document = self.__get_document(docid, node_connection)
        self.__close_connection(node_connection)
        document_data = document['document']
        meta = json.loads(document['meta'])
        if meta['compression']:
            document_data = self.__decompress_document(document_data)
        print document_data

    def do_set(self, docid, document_data):
        node_connection = self.__build_node_connection(docid)
        document = self.__get_document(docid, node_connection)
        document_data, size = self.__modify_size(document_data)
        meta = json.loads(document['meta'])
        if meta['compression']:
            document_data = self.__compress_document(document_data)
        doc_type = self.__get_doc_type(docid)
        table = "data_{}".format(doc_type)
        sql = "UPDATE {} SET document=%s WHERE docid=%s".format(table)
        cursor = node_connection.cursor()
        try:
            effect_count = cursor.execute(sql, (document_data, docid))
        except ProgrammingError:
            logger.error('database or table not exists, please confirm your docid is valid.')
            self.error_exit()
        except OperationalError:
            logger.error('error occurred during performed update sql, docid may not invalid.')
            self.error_exit()
        if effect_count == 0:
            logger.info("no row was updated, maybe it's not exists")
        node_connection.commit()
        self.__close_connection(node_connection)


def parse_arguments():
    parser = argparse.ArgumentParser(prog="illdb_browser")
    parser.add_argument("--show-sql", dest="show_sql", help="print SQL command to stderr", action="store_true")
    parser.add_argument("--port", type=int, default=3306, help="database port")
    parser.add_argument("--host", type=str, help="database host")
    parser.add_argument("--bucket", type=str, help="database bucket name")
    parser.add_argument("-u", "--username", type=str, help="database username")
    parser.add_argument("-p", "--password", type=str, help="database password")
    parent_parser = argparse.ArgumentParser(add_help=False)
    subparser = parser.add_subparsers(title='COMMANDS')
    get_parse = subparser.add_parser('get', parents=[parent_parser])
    get_parse.set_defaults(command='get')
    get_parse.add_argument("docid", type=str)
    set_parse = subparser.add_parser('set', parents=[parent_parser])
    set_parse.set_defaults(command='set')
    set_parse.add_argument('docid', type=str)
    set_parse.add_argument("document", type=str)
    return parser.parse_args()


def main():
    args = parse_arguments()
    handler = Handler(**vars(args))
    handler.handle()


if __name__ == "__main__":
    main()
