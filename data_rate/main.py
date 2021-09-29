# coding=utf-8
import pymysql
import csv

if __name__ == '__main__':
    db = pymysql.connect(host="192.1.4.202", user="root", password="123456", database="lib2021")
    cursor = db.cursor()

    # 获取表结构
    sql = "show columns from modify_title_info_hust_ori_2021;"
    cursor.execute(sql)
    table = cursor.fetchall()
    table_names = [t[0] for t in table]


    # 获取每种类别数据的覆盖率
    def get_type_rate(literature_type):
        f = open("type={}.csv".format(literature_type), "w", encoding="utf=8", newline="")
        csv_writer = csv.writer(f)
        csv_writer.writerow(['colname', 'rate'])
        sql = "select count(*) from modify_title_info_hust_ori_2021 where type = {};".format(literature_type)
        cursor.execute(sql)
        total = cursor.fetchone()[0]
        for col in table_names:
            sql = "select count(*) from `modify_title_info_hust_ori_2021` where type = {} and {} is not NULL and {} != '';".format(
                literature_type, col, col)
            cursor.execute(sql)
            num = cursor.fetchone()[0]
            rate = num / total
            csv_writer.writerow([col, rate])
            print("type = {}  {} rate= {}".format(literature_type, col, rate))
        f.close()


    for i in range(1, 15):
        get_type_rate(i)

    # 关闭数据库连接
    db.close()
