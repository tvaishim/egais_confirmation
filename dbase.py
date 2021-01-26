import sqlite3
import time
import _thread as thread


class DB:
    def __init__(self):
        self.connect = sqlite3.connect("data.db", check_same_thread=False)
        self.connect.row_factory = sqlite3.Row  # Установим вывод запросов не в tuple,
                                                # а в sqlite3.Row с заголовками
        self.cur = self.connect.cursor()
        self.mutex = thread.allocate_lock()
        # self.clear_skipped()

    def get_system(self):
        self.cur.execute("SELECT * FROM system WHERE id=1")
        return dict(self.cur.fetchone())

    def log(self, vid, url, data, error, result, status, text):
        date = time.strftime("%Y-%m-%d %X", time.localtime(time.time()))
        self.cur.execute("INSERT INTO logs(date, vid, url, data, error, result, status, text) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         (date, vid, url, data, error, result, status, text))
        self.connect.commit()

    def add_queue(self, vid, url, data):
        self.cur.execute("SELECT * FROM queue WHERE (vid=?) and (url=?) and (data=?)", (vid, url, data))
        if self.cur.fetchone() is None:
            date = time.strftime("%Y-%m-%d %X", time.localtime(time.time()))
            self.cur.execute("INSERT INTO queue(date, vid, url, data) \
                             VALUES (?, ?, ?, ?)",
                             (date, vid, url, data))
            self.connect.commit()

    def get_queue(self):
        self.cur.execute("SELECT * FROM queue ORDER BY date LIMIT 1")
        return self.cur.fetchone()

    def del_queue(self, pid):
        self.cur.execute("DELETE FROM queue WHERE pid=?", (pid,))
        self.connect.commit()

    def add_requests(self, url, data, url_id):
        date = time.strftime("%Y-%m-%d %X", time.localtime(time.time()))
        self.cur.execute("INSERT INTO requests(date, url, data, url_id) \
                         VALUES (?, ?, ?, ?)",
                         (date, url, data, url_id))
        self.connect.commit()

    def set_system(self, field, value):
        self.cur.execute("UPDATE system SET {}=? WHERE id=1".format(field), (value, ))
        self.connect.commit()

    def in_request_data(self, url_id):
        self.cur.execute("SELECT * FROM requests WHERE url_id=?", (url_id,))
        return self.cur.fetchone()

    def clear_skipped(self):
        self.cur.execute("DELETE FROM skipped")
        self.connect.commit()

    def in_skipped(self, url):
        self.cur.execute("SELECT * FROM skipped WHERE url=?", (url,))
        return self.cur.fetchone()

    def add_skipped(self, url):
        self.cur.execute("INSERT INTO skipped(url) VALUES (?)", (url,))
        self.connect.commit()

    def add_answers(self, url_id, data):
        date = time.strftime("%Y-%m-%d %X", time.localtime(time.time()))
        self.cur.execute("INSERT INTO answers(date, url_id, data) VALUES (?, ?, ?)",
                         (date, url_id, data))
        self.connect.commit()

    def get_answers(self):
        self.cur.execute("SELECT * FROM answers WHERE status<>1 ORDER BY date")
        return self.cur.fetchall()

    def set_answers(self, pid, field, value):
        self.cur.execute("UPDATE answers SET {}=? WHERE pid=?".format(field), (value, pid))
        self.connect.commit()

    def write_tab_nattn(self, records):
        self.cur.execute("DELETE FROM nattn")
        self.connect.commit()
        self.cur.executemany("INSERT INTO nattn(doc_data, doc_num, ttn, shipper) VALUES(?, ?, ?, ?)", records)
        self.connect.commit()
        return True

    def get_nattn(self):
        self.cur.execute("SELECT * FROM nattn WHERE (sel=1) and (req=0) and (ans=0) ORDER BY doc_data")
        return self.cur.fetchone()

    def get_nattn2(self, ttn):
        self.cur.execute("SELECT * FROM nattn WHERE ttn=?", (ttn,))
        return self.cur.fetchone()

    def get_nattn3(self, doc_data, doc_num, shipper):
        self.cur.execute("SELECT * FROM nattn WHERE (doc_data=?) and (doc_num=?) and (shipper=?)", (doc_data, doc_num, shipper))
        return self.cur.fetchone()

    def get_nattn4(self):
        self.cur.execute("SELECT * FROM nattn WHERE (doc1>0) and (doc2>0) and (ans=1) and (pod_req=0) ORDER BY pid")
        return self.cur.fetchall()

    def set_nattn(self, pid, field, value):
        self.cur.execute("UPDATE nattn SET {}=? WHERE pid=?".format(field), (value, pid))
        self.connect.commit()

    def nattn_show_data(self):
        self.cur.execute("SELECT pid,sel,doc_data,doc_num,ttn,shipper,req,ans,doc1,doc2,pod_req,pod_t1,pod_t2 FROM nattn")
        return self.cur.fetchall()

    def nattn_report(self):
        self.cur.execute("SELECT COUNT(*) FROM nattn")
        all_count = tuple(self.cur.fetchone())[0]
        self.cur.execute("SELECT COUNT(*) FROM nattn WHERE ans>0")
        ans_count = tuple(self.cur.fetchone())[0]
        self.cur.execute("SELECT COUNT(*) FROM nattn WHERE (pod_req>0)and(pod_t1>0)and(pod_t2>0)")
        pod_count = tuple(self.cur.fetchone())[0]
        return "\n===========================================\nВсего неподтвержденных документов {}\nЗапрошено документов {}\nПодтверждено документов {}\n===========================================\n\n".format(all_count, ans_count, pod_count)

db = DB()

if __name__ == '__main__':
    a = db.get_system()
    print(type(a))
    print(a)


    a = db.nattn_show_data()


    print(type(a))
    print(a)
    print(a[0])
    print(dict(a[0]))
    print(tuple(a[0]))


