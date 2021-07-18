#!/usr/bin/env python
"""
说明：此脚本用于启动和控制redis的GUI界面
作者：huangjunhao
日期：2021-05-24
"""
import sys
import json
import re
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from connection import Ui_Connection
from window import Ui_RedisDesktop
from redis_operation import test_connection, RedisOperation, transform_key_name_to_simple


SHOW_ROW_NUMBER = 100


def up_window_im_by_bool(instance, status, true_title, true_im, false_title, false_im):
    """
    :param instance: 窗口类的实例
    :param status: 状态
    :param true_title: 状态为真的弹窗标题
    :param true_im: 状态为真的弹窗信息
    :param false_title: 状态为假的弹窗标题
    :param false_im: 状态为假的弹窗信息
    :return:
    """
    if status:
        QMessageBox.information(instance, true_title, true_im, QMessageBox.Yes)
    else:
        QMessageBox.information(instance, false_title, false_im, QMessageBox.Yes)


def up_window_question_by_bool(instance, title, im):
    """
    :param instance: 窗口类的实例
    :param title: 弹窗标题
    :param im: 弹窗信息
    :return: 是否点击了确认键
    """
    reply = QMessageBox.question(instance, title, im, QMessageBox.Yes | QMessageBox.No)
    return reply == QMessageBox.Yes


class ChildWindow(QMainWindow, Ui_Connection):
    def __init__(self, parent_instance):
        """
        窗口初始化,按钮和方法之间的绑定
        :param parent_instance: 窗口父类的实例
        """
        super(ChildWindow, self).__init__()
        self.setupUi(self)
        self.parent_instance = parent_instance
        self.test_connection_button.clicked.connect(self.detect_button)
        self.save_button.clicked.connect(self.detect_button)

    @staticmethod
    def save_connection(name, ip, port, auth):
        """
        存储redis的连接信息至redis.txt
        :param name: 连接名
        :param ip: 连接ip
        :param port: 连接端口
        :param auth: 连接密码
        :return:
        """
        with open("redis.txt", "a") as f:
            f.write(json.dumps({"name": name, "ip": ip, "port": port, "auth": auth}) + '\n')

    def detect_button(self):
        """
        监测哪个button被点击,然后执行对应的函数
        :return:
        """
        sender = self.sender()
        if sender.text() == "Test Connection":
            if self.host_input.text() == "" or self.port_input.text() == "":
                up_window_im_by_bool(self, False, "", "", "Invalid settings", "Invalid settings detected")
            else:
                connect_status = test_connection(self.host_input.text(), self.port_input.text(), self.auth_input.text())
                up_window_im_by_bool(self, connect_status, "Successful connection", "Successful connection to redis",
                                     "Can't connect to redis-server", "Can't connect to redis-server")
        elif sender.text() == "OK":
            if self.host_input.text() == "" or self.port_input.text() == "":
                up_window_im_by_bool(self, False, "", "", "Invalid settings", "Invalid settings detected")
            else:
                self.save_connection(self.name_input.text(), self.host_input.text(), self.port_input.text(),
                                     self.auth_input.text())
                redis_name_list, self.parent_instance.redis_im_dict = self.parent_instance.get_redis_im()
                self.parent_instance.connection_name.clear()
                self.parent_instance.connection_name.addItems(redis_name_list)  # 更新所有im连接信息
                self.close()


class ParentWindow(QMainWindow, Ui_RedisDesktop):
    def __init__(self):
        super(ParentWindow, self).__init__()
        self.setupUi(self)
        self.model = object()
        self.con = object()  # 当前redis连接实例
        self.cur_db = '0'  # 当前的db
        self.cur_key_deleted_number = 0  # 当前key被删除的数据行数(一定小于等于常量SHOW_ROW_NUMBER)
        self.cur_key_length = 0   # 当前key的数据长度
        self.key_name = None  # 当前key_name
        self.key_type = None  # 当前key_type
        self.all_db_keys_list = list()  # 所有db及其key的数量
        self.all_db_keys_number_dict = dict()  # 所有db及其的key所有种类及数量 {0:{key:number}}
        # 所有db下的五种数据结构key的数量 {0:{"string":0,"list":0,"hash":0,"set":0,"zset":0}}
        redis_name_list, self.redis_im_dict = self.get_redis_im()
        self.connection_name.addItems(redis_name_list)  # 加入所有im连接信息
        self.select_connection_button.clicked.connect(self.select_all_db_keys_number)  # 统计当前连接方式下所有db的key的数量
        self.select_db_button.clicked.connect(self.current_db_show_all_keys_number)  # 统计当前db的不同key种类的数量
        self.select_button.clicked.connect(self.show_data)  # 展示当前key下的常量条数据
        self.show_data_dict = dict()  # 临时保存数据字典,为了保存操作进行准备 {row:dict()/str()}
        self.save_value_button.clicked.connect(self.edit_save_data)  # 编辑然后存储当前数据
        self.delete_row_button.clicked.connect(self.delete_row)  # 删除当前数据
        self.delete_key_button.clicked.connect(self.delete_key)  # 删除当前key

    @staticmethod
    def get_redis_im():
        redis_name_list = []
        redis_im_dict = {}
        with open('redis.txt', 'r') as f:
            a = f.readlines()
            for i in a:
                temp_dict = json.loads(i.strip())
                redis_name_list.append(temp_dict['name'])
                redis_im_dict[temp_dict['name']] = {'ip': temp_dict['ip'], 'port': temp_dict['port'],
                                                    'auth': temp_dict['auth']}
        return redis_name_list, redis_im_dict

    #  获取当前连接方式下的redis,所有db的key数量
    def select_all_db_keys_number(self):
        self.all_db_keys_list.clear()
        for i in range(0, 16):
            con = RedisOperation(redis_db=i, redis_host=self.redis_im_dict[self.connection_name.currentText()]['ip'],
                                 redis_port=self.redis_im_dict[self.connection_name.currentText()]['port'],
                                 password=self.redis_im_dict[self.connection_name.currentText()]['auth'])
            key_number_dict = con.scan_all_keys()
            self.all_db_keys_number_dict[str(i)] = key_number_dict
            db_key_number = sum(key_number_dict.values())
            self.all_db_keys_list.append('db{} ({})'.format(i, db_key_number))
        self.connection_db.clear()
        self.connection_db.addItems(self.all_db_keys_list)

    #  发生了key删除或者row删除后key消失的情况后,更新db的key数量
    def cur_db_keys_number_reduce(self):
        self.all_db_keys_list.clear()
        self.all_db_keys_number_dict[self.cur_db][transform_key_name_to_simple(self.key_name)] -= 1
        if self.all_db_keys_number_dict[self.cur_db][transform_key_name_to_simple(self.key_name)] == 0:
            self.all_db_keys_number_dict[self.cur_db].pop(transform_key_name_to_simple(self.key_name))
        for i in range(0, 16):
            db_key_number = sum(self.all_db_keys_number_dict[str(i)].values())
            self.all_db_keys_list.append('db{} ({})'.format(i, db_key_number))
        self.connection_db.clear()
        self.connection_db.addItems(self.all_db_keys_list)

    #  统计当前db各类的key数量
    def current_db_show_all_keys_number(self):
        column_list = ['key','value']
        self.cur_db = re.findall("db(\\d+) ", self.connection_db.currentText())[0]
        key_number_dict = self.all_db_keys_number_dict[self.cur_db]
        row_number = len(key_number_dict)
        self.create_model(row_number, *column_list)
        cur_row = 0
        for key, number in key_number_dict.items():
            self.model.setItem(cur_row, 0, QStandardItem(str(key)))
            self.model.setItem(cur_row, 1, QStandardItem(str(number)))
            cur_row += 1
        self.tableView.setModel(self.model)
        self.key_type_view.setText('key统计')
        # 水平方向标签拓展剩下的窗口部分，填满表格
        # self.key_number_view.horizontalHeader().setStretchLastSection(True)
        # 水平和垂直方向，表格大小拓展到适当的尺寸
        # self.key_number_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        # self.key_number_view.verticalHeader().setSectionResizeMode(QHeaderView.Stretch)

    #  创建一个展示模板
    def create_model(self, row_number, *args):
        """
        :param row_number: 行数量
        :param args: 字段标题
        """
        self.model = QStandardItemModel(row_number, len(args))
        self.model.setHorizontalHeaderLabels(list(args))

    #  查找redis并展示中该key中的值
    def show_data(self):
        self.cur_key_deleted_number = 0  # 将删除数据置为0
        self.con = RedisOperation(redis_db=self.cur_db,
                                  redis_host=self.redis_im_dict[self.connection_name.currentText()]['ip'],
                                  redis_port=self.redis_im_dict[self.connection_name.currentText()]['port'],
                                  password=self.redis_im_dict[self.connection_name.currentText()]['auth'])
        self.key_name = self.key_edit.text()
        print('当前的key:' + self.key_name)
        self.key_type = self.con.get_type(self.key_name)
        if not self.key_type:  # key_type为None则代表该key并不存在,抛出警告
            up_window_im_by_bool(self, False, "", "", "not existed", "key is not existed")
        self.key_type_view.setText(self.key_type)
        if self.key_type == "list":
            column_list = ["value"]
            self.create_model(SHOW_ROW_NUMBER, *column_list)
            data_generator = self.con.lrange(self.key_name)
            self.cur_key_length = self.con.llen(self.key_name)
            for r_num in range(SHOW_ROW_NUMBER):
                try:
                    self.model.setItem(r_num, 0, QStandardItem((data_generator.__next__()).decode()))
                except StopIteration:
                    break
            self.tableView.setModel(self.model)
        elif self.key_type == "string":
            row_number = 0
            column_list = ["value"]
            self.create_model(row_number, *column_list)
            self.cur_key_length = 1
            self.model.setItem(0, 0, QStandardItem(self.con.get(self.key_name)))
            self.tableView.setModel(self.model)
        elif self.key_type == "zset":
            column_list = ["key", "value"]
            self.create_model(SHOW_ROW_NUMBER, *column_list)
            data_generator = self.con.zscan_iter(self.key_name)
            self.cur_key_length = self.con.zcard(self.key_name)
            for r_num in range(SHOW_ROW_NUMBER):
                try:
                    temp_tuple = data_generator.__next__()
                    self.model.setItem(r_num, 0, QStandardItem(temp_tuple[0].decode()))
                    self.model.setItem(r_num, 1, QStandardItem(str(temp_tuple[1])))
                    self.show_data_dict[r_num] = temp_tuple[0].decode()  # 保存{行号:value}
                except StopIteration:
                    break
            self.tableView.setModel(self.model)
        elif self.key_type == "set":
            column_list = ["value"]
            self.create_model(SHOW_ROW_NUMBER, *column_list)
            data_generator = self.con.sscan_iter(self.key_name)
            self.cur_key_length = self.con.scard(self.key_name)
            for r_num in range(SHOW_ROW_NUMBER):
                try:
                    temp_data = data_generator.__next__().decode()
                    self.model.setItem(r_num, 0, QStandardItem(temp_data))
                    self.show_data_dict[r_num] = temp_data  # 保存{行号:value}
                except StopIteration:
                    break
            self.tableView.setModel(self.model)
        elif self.key_type == "hash":
            column_list = ["key", "value"]
            self.create_model(SHOW_ROW_NUMBER, *column_list)
            data_generator = self.con.hscan_iter(self.key_name)
            self.cur_key_length = self.con.hlen(self.key_name)
            for r_num in range(SHOW_ROW_NUMBER):
                try:
                    temp_tuple = data_generator.__next__()
                    self.model.setItem(r_num, 0, QStandardItem(temp_tuple[0].decode()))
                    self.model.setItem(r_num, 1, QStandardItem(temp_tuple[1].decode()))
                    self.show_data_dict[r_num] = temp_tuple[0].decode()  # 保存{行号:value}
                except StopIteration:
                    break
            self.tableView.setModel(self.model)
        self.key_size_view.setText(str(self.cur_key_length))

    #  删除当前db中的某key
    def delete_key(self):
        result = up_window_question_by_bool(self, 'Delete key', 'Do you really want to delete this key?')
        if result and self.key_name:
            result = self.con.delete(self.key_name)
            if result:
                self.tableView.setModel(None)
                self.cur_db_keys_number_reduce()
                self.key_name = None

    #  删除:返回要删除的数据
    def select_indexs_to_delete(self):
        if self.tableView.selectionModel():
            indexs = self.tableView.selectionModel().selection().indexes()
        else:
            return
        values_list = []
        index_row_list = []
        if len(indexs) > 0:
            for index in indexs:
                index_row_list.append(index.row())
            index_row_list = list(set(index_row_list))
            for index in index_row_list:
                values_list.append(self.model.item(index, column=0).text())
            return values_list, sorted(index_row_list, reverse=True)
        else:
            return None, None

    #  删除当前db中的某key的一行或多行数据
    def delete_row(self):
        """
        删除当前的某行或多行数据
        :return:
        """
        if self.key_type == "string":
            self.delete_key()
            self.model.removeRows(0, 1)
            self.cur_db_keys_number_reduce()
        elif self.key_type:
            values_list, index_row_list = self.select_indexs_to_delete()
            if values_list:
                result = up_window_question_by_bool(self, 'Delete row', 'Do you really want to delete these rows?')
                if not result:
                    return
                if self.key_type == "hash":
                    self.con.hdel(self.key_name, *values_list)
                elif self.key_type == "set":
                    self.con.srem(self.key_name, *values_list)
                elif self.key_type == "zset":
                    self.con.zrem(self.key_name, *values_list)
                elif self.key_type == "list":
                    for value in values_list:  # 从队列头至尾,删除第一个碰到的value
                        self.con.lrem(self.key_name, value)
                else:
                    return
                self.cur_key_deleted_number += len(values_list)
                #  删除界面上的展示数据
                for index in index_row_list:
                    self.model.removeRows(index, 1)
                self.key_size_view.setText(str(self.cur_key_length-self.cur_key_deleted_number))
                #  假如被删除的数据量等于key中数据量,则需重新更新db中key的数量
                if self.cur_key_deleted_number == self.cur_key_length:
                    self.cur_db_keys_number_reduce()
                    self.key_name = None

    #  修改:返回要修改的数据
    def select_indexs_to_save(self, limit_length):
        if self.tableView.selectionModel():
            indexs = self.tableView.selectionModel().selection().indexes()
        else:
            return
        values_list = []
        print(indexs)
        if 0 < len(indexs) <= 2 and len(indexs) == limit_length:
            # 必须为同一行的数据 self.key_type == "hash" or self.key_type == "zset"
            if len(indexs) == 2 and indexs[0].row() == indexs[1].row():
                values_list.append(indexs[0].row())
                values_list.append(self.model.item(indexs[0].row(), column=0).text())
                values_list.append(self.model.item(indexs[0].row(), column=1).text())
            elif len(indexs) == 1 and self.key_type == "string":
                values_list.append(self.model.item(indexs[0].row(), column=0).text())
            elif self.key_type == "list" or self.key_type == "set":
                values_list.append(indexs[0].row())
                values_list.append(self.model.item(indexs[0].row(), column=0).text())
            return values_list
        else:
            up_window_im_by_bool(self, False, "", "", "错误", "需要选择单行数据！")

    #  修改并存储数据
    def edit_save_data(self):
        """
        编辑并存储单条数据,由于情况种类太多.目前hash,set,zset仅通过展示时存储的{行数：数据}来进行 删除添加操作
        :return:
        """
        if self.key_type == "string":
            values_list = self.select_indexs_to_save(1)
            if values_list:
                self.con.set(self.key_name, values_list[0])
        elif self.key_type == "list":
            values_list = self.select_indexs_to_save(1)
            if values_list:
                self.con.lset(self.key_name, values_list[0], values_list[1])
        elif self.key_type == "hash":
            values_list = self.select_indexs_to_save(2)
            if values_list:
                self.con.hdel(self.key_name, self.show_data_dict[values_list[0]][0])  # 删除
                self.con.hset(self.key_name, values_list[1], values_list[2])  # 增加
        elif self.key_type == "set":
            values_list = self.select_indexs_to_save(1)
            if values_list:
                self.con.srem(self.key_name, self.show_data_dict[values_list[0]])  # 删除
                self.con.sadd(self.key_name, values_list[1])  # 增加
        elif self.key_type == "zset":
            values_list = self.select_indexs_to_save(2)
            if values_list:
                self.con.zrem(self.key_name, self.show_data_dict[values_list[0]][0])  # 删除
                self.con.zadd(self.key_name, {values_list[1]:values_list[2]})  # 增加
        else:
            return
        up_window_im_by_bool(self, True, "Save value", "Value was updated", "", "")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    myWin = ParentWindow()
    myWin.child_window = ChildWindow(myWin)  # 父窗口关联子窗口
    myWin.add_connection_button.clicked.connect(myWin.child_window.show)
    myWin.show()
    sys.exit(app.exec_())

