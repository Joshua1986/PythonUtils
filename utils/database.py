# coding=utf-8
import logging
import logger
from contextlib import closing
import MySQLdb
import time
import datetime
from decimal import Decimal
from _mysql_exceptions import OperationalError


class database:
    def __init__(self, host="127.0.0.1", user='root', passwd='pwd123456', port=3306, db="whatif", logger="whatif"):
        try:
            self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, port=port,  db=db, charset='utf8')
        except OperationalError:
            time.sleep(5)
            self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, port=port,  db=db, charset='utf8')
        finally:
            self.logger = logging.getLogger(logger)

    # def __del__(self):
    #     # self.cur.close()
    #     self.conn.close()

    # 基础方法，用来执行任意SQL
    def execute_sql(self, sql):
        res = []
        try:
            self.logger.info("SQL to be executed:")
            self.logger.info(sql)
            with closing(self.conn.cursor()) as cur:
                if type(sql) == list:
                    for esql in sql:
                        cur.execute(esql)
                        res.append(cur.fetchall())
                elif type(sql) == str:
                    cur.execute(sql)
                    res.append(cur.fetchall())
                else:
                    self.logger.error("The type of sql is wrong, check the sql below:\n{0}".format(sql))
                self.conn.commit()
        except MySQLdb.Error, e:
            self.logger.error("Mysql Error : {0}".format(e.args))
        finally:
            return res

    # 基础方法，用户查询第一列数据
    def select_sql(self, sql):
        try:
            with closing(self.conn.cursor()) as cur:
                count = cur.execute(sql)
                self.logger.info(u"共查询到{0}条数据".format(count))
                s = []
                if count == 0:
                    self.logger.info(u"没有查询到数据，返回空列表！")
                    return []
                results = cur.fetchall()
                for ifOrderInfo in results:
                    s.append(ifOrderInfo)

                self.logger.info(s)
                return [x[0] for x in s]
        except MySQLdb.Error, e:
            self.logger.error("Mysql Error : {0}".format(e.args))

    # 返回一个表中的特定字段的第一个数据
    # 例子：
    # select_sql_with_stm("dsp_info.dsp_name")
    #
    # return：
    # 厦门碧水新村度假酒店7
    def select_sql_with_stm(self, column, stm=""):
        assert ("." in column)
        assert isinstance(column, (str, str, str))

        stm = str("where " + stm) if stm else ""
        s = str(column).split(".")
        sql = "select %s from %s %s" % (s[1], s[0], stm)
        self.logger.info(sql)
        result = self.select_sql(sql)
        if not result:
            return ''
        else:
            return result[0]

    # 返回一个表中的特定字段的所有数据，返回一个列表
    # 例子：
    # select_sql_with_stm_whole("dsp_info.dsp_name")
    #
    # return：
    # ['厦门碧水新村度假酒店7', '董沐测试']
    def select_sql_with_stm_whole(self, column, stm=""):
        assert ("." in column)
        assert isinstance(column, (str, str, str))

        stm = str("where " + stm) if stm else ""
        s = str(column).split(".")
        sql = "select %s from %s %s" % (s[1], s[0], stm)
        #     sql = "select %s from %s %s order by %s"%(s[1],s[0],stm,s[1])
        self.logger.info(sql)
        return self.select_sql(sql)

    # 输入一个dictionary格式的数据，向指定的数据库中插入一条数据
    # 例子：
    # insert_sql("dsp_info"， XXX_dict)
    #
    # return：
    # 插入的数据
    def insert_sql(self, table, sql_dict):
        assert isinstance(sql_dict, dict)

        key_string = ",".join(sql_dict.keys())
        value_list = []
        for ifOrderInfo in sql_dict.keys():
            value_list.append(str(sql_dict[ifOrderInfo]))
        value_string = "'{0}'".format("','".join(value_list))
        sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(str(table), key_string, value_string)
        self.logger.info(sql)
        return self.execute_sql(sql)

    # 查询表中数据，返回一个dict格式的数据，只限于一条数据
    # 例子：
    # select_single_to_dict_one("dsp_info")
    #
    # return：
    # {'XXXX_account': u'gg', 'status': 0, 'update_time': None, 'dsp_phone': u''......
    def select_single_to_dict_one(self, table, columns="*", stm=""):
        stm = str("where " + stm) if stm else ""
        sql = "select {0} from {1} {2}".format(columns, table, stm)
        self.logger.info(sql)
        result = self.query(sql)
        for key in result[0]:
            if isinstance(result[0][key], datetime.datetime):
                result[0][key] = str(result[0][key])
            if isinstance(result[0][key], Decimal):
                result[0][key] = float(result[0][key])
            if isinstance(result[0][key], long):
                result[0][key] = int(result[0][key])
        return result[0] if result else {}

    # 查询表中数据，返回一个dict格式的数据，返回list数据，list中是用dict形式展示的数据
    # 是对select_single_to_dict_one函数的扩展
    # 例子：
    # select_single_to_list("dsp_info")
    #
    # return：
    # [{'XXXX_account': u'gg', 'status': 0, 'update_time': None, 'dsp_phone': u''......]
    def select_to_list(self, table, columns="*", stm=""):
        stm = str("where " + stm) if stm else ""
        sql = "select {0} from {1} {2}".format(columns, table, stm)
        self.logger.info(sql)
        results = self.query(sql)
        if results:
            for result in results:
                for key in result:
                    if isinstance(result[key], datetime.datetime):
                        result[key] = str(result[key])
                    if isinstance(result[key], Decimal):
                        result[key] = float(result[key])
                    if isinstance(result[key], long):
                        result[key] = int(result[key])
            return results
        else:
            return {}

    # 基础方法，查询全部数据
    def query(self, sql):
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql)
            self.logger.info(sql)
            index = cur.description
            index = [x[0] for x in index]
            self.logger.info(index)
            result = []
            for res in cur.fetchall():
                row = {}
                for ifOrderInfo in range(len(index)):
                    row[index[ifOrderInfo]] = res[ifOrderInfo]
                result.append(row)
            self.logger.info(result)
            return result

    # 给数据库改名的优化方法
    def rename_database(self, old_db, new_db):
        self.execute_sql("CREATE DATABASE {0}".format(new_db))
        table_list = self.query("SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{0}';".format(old_db))
        table_list = [x["table_name"] for x in table_list]
        ts = []
        for table in table_list:
            ts.append("{0}.{2} TO {1}.{2}".format(old_db, new_db, table))
        if ts:
            self.execute_sql("RENAME TABLE " + ",".join(ts))
            self.execute_sql("DROP DATABASE {0};".format(old_db))
        else:
            self.logger.error(u"Fail to rename {0}".format(old_db))

    def close_connection(self):
        # self.cur.close()
        self.conn.close()


if __name__ == "__main__":
    pass


