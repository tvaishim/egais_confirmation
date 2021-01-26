import requests
from bs4 import BeautifulSoup
import dbase
import _thread as thread
import time
import configparser


class Req:
    def __init__(self):
        self.utm_url = ""
        self.utm_port = ""
        self.fsrarid = ""
        self.confirm_at_once = False

        self.utm_online = False
        self.utm_fsrarid = ""
        self.message = ""
        self.load_settings()
        self.start()
        self.diagnosis()

    def load_settings(self):
        config = configparser.ConfigParser()
        config.read("settings.ini")
        self.utm_url = config.get("UTM", "adress")
        self.utm_port = config.get("UTM", "port")
        if config.get("OTHER", "confirm_at_once").upper() == "TRUE":
            self.confirm_at_once = True

    def print_text(self, text):
        self.message += "{}\n".format(text)

    def start(self):
        thread.start_new_thread(self.run_queue, ())
        thread.start_new_thread(self.run_request_data, ())
        thread.start_new_thread(self.run_answer_data, ())
        thread.start_new_thread(self.run_tnn, ())

    def run_request_data(self):
        while True:
            if self.utm_online:
                # Если очередь не пуста, то подождем
                with dbase.db.mutex:
                    res = dbase.db.get_queue()
                if res is None:
                    # запросим входящие документы УТМ
                    res = self.request_utm("GET", "/opt/out")
                    if res["result"]:
                        # Обработаем ответ
                        soup = BeautifulSoup(res["text"], "xml")
                        tags = soup.find_all("url")
                        for tag in tags:
                            with dbase.db.mutex:
                                rows = dbase.db.in_skipped(tag.text)
                            if rows is None:
                                self.place_in_queue("GETDOC", tag.text, tag.attrs.get("replyId", ""))
            time.sleep(60)

    def run_queue(self):
        while True:
            if self.utm_online:
                # получим очередь
                with dbase.db.mutex:
                    res = dbase.db.get_queue()
                if res is not None:
                    queue = dict(res)
                    # исполним запрос
                    self.print_text("{} {}".format(queue["vid"], queue["url"]))
                    res = self.request_utm(queue["vid"], queue["url"], queue["data"])
                    if res["result"]:
                        # удалим запрос из очереди
                        with dbase.db.mutex:
                            dbase.db.del_queue(queue["pid"])
                        # Обработаем ответ
                        self.work_queue(res)

            time.sleep(1)

    def run_answer_data(self):
        while True:
            # получим очередь
            with dbase.db.mutex:
                ress = dbase.db.get_answers()
            for res in ress:
                answer = dict(res)
                self.work_answer_data(answer)
            if self.confirm_at_once:
                self.do_confirm_ttn()
            time.sleep(60)

    def run_tnn(self):
        while True:
            if self.utm_online:
                with dbase.db.mutex:
                    system_data = dbase.db.get_system()
                if (time.time() - system_data["req_ttn_data"]) > 12 * 60:
                    with dbase.db.mutex:
                        res = dbase.db.get_nattn()
                    if res is not None:
                        ttn = dict(res)
                        self.queryttn(ttn["ttn"])
                        with dbase.db.mutex:
                            dbase.db.set_nattn(ttn["pid"], "req", 1)

            time.sleep(60)

    def work_answer_data(self, answer):
        soup = BeautifulSoup(answer["data"], "xml")
        tags = soup.Documents.Document.contents
        # Удалим пустые элементы
        for i in range(len(tags) - 1, -1, -1):
            if str(tags[i]).isspace():
                tags.pop(i)
        tag = tags[0]
        vid_doc = tag.name

        with dbase.db.mutex:
            dbase.db.set_answers(answer["pid"], "vid_doc", vid_doc)

        if vid_doc == "Ticket":
            tag_doctype = tag.DocType.text
            if tag_doctype == "QueryNATTN":
                with dbase.db.mutex:
                    dbase.db.set_answers(answer["pid"], "status", 1)

            elif tag_doctype == "QueryResendDoc":
                with dbase.db.mutex:
                    rows = dbase.db.in_request_data(answer["url_id"])
                if rows is not None:
                    row = dict(rows)
                    req_soup = BeautifulSoup(row["data"], "xml")
                    ttn = req_soup.find("Value").text
                    with dbase.db.mutex:
                        rows = dbase.db.get_nattn2(ttn)
                    if rows is not None:
                        row = dict(rows)
                        #a = time.mktime(time.strptime(t.split(".")[0], "%Y-%m-%dT%X")
                        if tag.Result.Conclusion.text == "Accepted":
                            with dbase.db.mutex:
                                dbase.db.set_nattn(row["pid"], "ans", 1)
                        elif tag.Result.Conclusion.text == "Rejected":
                            with dbase.db.mutex:
                                dbase.db.set_nattn(row["pid"], "req", 0)
                        with dbase.db.mutex:
                            dbase.db.set_answers(answer["pid"], "status", 1)

            elif tag_doctype == "WayBillAct":
                with dbase.db.mutex:
                    rows = dbase.db.in_request_data(answer["url_id"])
                if rows is not None:
                    row = dict(rows)
                    req_soup = BeautifulSoup(row["data"], "xml")
                    ttn = req_soup.find("WBRegId").text
                    with dbase.db.mutex:
                        rows = dbase.db.get_nattn2(ttn)
                    if rows is not None:
                        row = dict(rows)
                        if tag.Result.Conclusion.text == "Accepted":
                            with dbase.db.mutex:
                                dbase.db.set_nattn(row["pid"], "pod_t1", 1)
                        else:
                            with dbase.db.mutex:
                                dbase.db.set_nattn(row["pid"], "pod_t1", 2)
                                dbase.db.set_nattn(row["pid"], "pod_req", 0)
                        with dbase.db.mutex:
                            dbase.db.set_answers(answer["pid"], "status", 1)

            elif tag_doctype == "WAYBILL":
                with dbase.db.mutex:
                    rows = dbase.db.in_request_data(answer["url_id"])
                if rows is not None:
                    row = dict(rows)
                    req_soup = BeautifulSoup(row["data"], "xml")
                    ttn = req_soup.find("WBRegId").text
                    with dbase.db.mutex:
                        rows = dbase.db.get_nattn2(ttn)
                    if rows is not None:
                        row = dict(rows)
                        if tag.OperationResult.OperationResult.text == "Accepted":
                            with dbase.db.mutex:
                                dbase.db.set_nattn(row["pid"], "pod_t2", 1)
                        else:
                            with dbase.db.mutex:
                                dbase.db.set_nattn(row["pid"], "pod_t2", 2)
                                dbase.db.set_nattn(row["pid"], "pod_req", 0)
                        with dbase.db.mutex:
                            dbase.db.set_answers(answer["pid"], "status", 1)

        elif vid_doc == "ReplyNoAnswerTTN":
            tags = soup.find_all("NoAnswer")
            records = []
            for tag in tags:
                records.append((tag.ttnDate.text, tag.ttnNumber.text, tag.WbRegID.text, tag.Shipper.text))
            with dbase.db.mutex:
                if dbase.db.write_tab_nattn(records):
                    dbase.db.set_answers(answer["pid"], "status", 1)

        elif vid_doc == "WayBill":
            with dbase.db.mutex:
                rows = dbase.db.get_nattn3(tag.Header.Date.text,
                                           tag.Header.NUMBER.text,
                                           tag.Header.Shipper.ClientRegId.text)
            if rows is not None:
                row = dict(rows)
                with dbase.db.mutex:
                    dbase.db.set_nattn(row["pid"], "doc1", 1)
                    dbase.db.set_answers(answer["pid"], "status", 1)

        elif vid_doc == "WayBill_v2":
            with dbase.db.mutex:
                rows = dbase.db.get_nattn3(tag.Header.Date.text,
                                           tag.Header.NUMBER.text,
                                           tag.Header.Shipper.ClientRegId.text)
            if rows is not None:
                row = dict(rows)
                with dbase.db.mutex:
                    dbase.db.set_nattn(row["pid"], "doc1", 2)
                    dbase.db.set_answers(answer["pid"], "status", 1)

        elif vid_doc == "WayBill_v3":
            with dbase.db.mutex:
                rows = dbase.db.get_nattn3(tag.Header.Date.text,
                                           tag.Header.NUMBER.text,
                                           tag.Header.Shipper.ClientRegId.text)
            if rows is not None:
                row = dict(rows)
                with dbase.db.mutex:
                    dbase.db.set_nattn(row["pid"], "doc1", 3)
                    dbase.db.set_answers(answer["pid"], "status", 1)

        elif vid_doc == "TTNInformBReg":
            with dbase.db.mutex:
                rows = dbase.db.get_nattn2(tag.Header.WBRegId.text)
            if rows is not None:
                row = dict(rows)
                with dbase.db.mutex:
                    dbase.db.set_nattn(row["pid"], "doc2", 1)
                    dbase.db.set_answers(answer["pid"], "status", 1)

        elif vid_doc == "TTNInformF2Reg":
            with dbase.db.mutex:
                rows = dbase.db.get_nattn2(tag.Header.WBRegId.text)
            if rows is not None:
                row = dict(rows)
                with dbase.db.mutex:
                    dbase.db.set_nattn(row["pid"], "doc2", 2)
                    dbase.db.set_answers(answer["pid"], "status", 1)

    def work_queue(self, res):
        if res["vid"] == "POST":
            # Получим url id запроса
            soup = BeautifulSoup(res["text"], "xml")
            url_id = soup.find("url").text
            # Запишем в таблицу запросов
            with dbase.db.mutex:
                dbase.db.add_requests(res["url"], res["data"], url_id)
        elif res["vid"] == "GET":
            if res["url"] == "/diagnosis":
                # Запрос диагностики
                soup = BeautifulSoup(res["text"], "xml")
                self.utm_fsrarid = soup.CERTIFICATE.CN.text
                self.fsrarid = soup.CERTIFICATE.CN.text
                self.utm_online = True
                if self.utm_fsrarid != self.fsrarid:
                    self.print_text("id настройки программы {} и id УТМ {} не совпадают!!!".format(self.fsrarid, self.utm_fsrarid))
                    self.utm_online = False
                self.print_text(self.utm_fsrarid)
            if res["url"] == "/opt/out":
                # запрос входящих документов
                soup = BeautifulSoup(res["text"], "xml")
                tags = soup.find_all("url")
                if tags:
                    for tag in tags:
                        # self.print_text("{} - {}".format(tag.text, tag.attrs.get("replyId", "")))
                        self.print_text(tag.text)
                else:
                    self.print_text("нет входящих документов")

        elif res["vid"] == "GETDOC":
            our_req = False
            if res["data"]:
                # Есть id ссылки, поищем ее в таблице запросов
                with dbase.db.mutex:
                    rows = dbase.db.in_request_data(res["data"])
                our_req = rows is not None
            else:
                # id ссылки нет, посмотрим виды документов

                soup = BeautifulSoup(res["text"], "xml")
                tags = soup.Documents.Document.contents
                # Удалим пустые элементы
                for i in range(len(tags) - 1, -1, -1):
                    if str(tags[i]).isspace():
                        tags.pop(i)
                tag = tags[0]
                vid_doc = tag.name

                if vid_doc == "WayBill":
                    with dbase.db.mutex:
                        rows = dbase.db.get_nattn3(tag.Header.Date.text,
                                                   tag.Header.NUMBER.text,
                                                   tag.Header.Shipper.ClientRegId.text)
                    our_req = rows is not None

                elif vid_doc == "TTNInformBReg":
                    with dbase.db.mutex:
                        rows = dbase.db.get_nattn2(tag.Header.WBRegId.text)
                    our_req = rows is not None

            if our_req:
                # Запишем в таблицу ответов
                with dbase.db.mutex:
                    dbase.db.add_answers(res["data"], res["text"])
                # Удалим входящий на УТМ
                self.place_in_queue("DEL", res["url"])
            else:
                with dbase.db.mutex:
                    dbase.db.add_skipped(res["url"])

    def place_in_queue(self, vid, url, data=""):
        # self.print_text(url)
        with dbase.db.mutex:
            dbase.db.add_queue(vid, url, data)

    def request_utm(self, vid, url, data=""):
        res = {"result": False, "status_code": 0, "text": "", "status": "",
               "vid": vid, "url": url, "data": data}
        error = ""
        try:
            with dbase.db.mutex:
                if vid == "GET":
                    r = requests.get("http://{}:{}{}".format(self.utm_url, self.utm_port, url))
                elif vid == "GETDOC":
                    r = requests.get(url)
                elif vid == "POST":
                    files = {'xml_file': ("select.xml", data)}
                    r = requests.post("http://{}:{}{}".format(self.utm_url, self.utm_port, url), files=files)
                elif vid == "DEL":
                    r = requests.delete(url)
        except Exception as err:
            res["status"] = "Ошибка запроса {}".format(err)
            error = str(err)
        else:
            res["status_code"] = r.status_code
            res["text"] = r.text
            if r.status_code == 200:
                res["result"] = True
                res["status"] = "ОК"
            else:
                res["status"] = "Запрос выполнен"

            #print(r.status_code)
            #print(r.text)

        with dbase.db.mutex:
            dbase.db.log(vid, url, data, error, str(res["result"]), res["status"], res["text"])

        return res

    def diagnosis(self):
        res = self.request_utm("GET", "/diagnosis")
        self.work_queue(res)
        # self.place_in_queue("GET", "/diagnosis")

    def req_utm_inner(self):
        self.place_in_queue("GET", "/opt/out")

    def querynattn(self):
        #self.print_text("запрос необработанных ттн")
        req_text = '<?xml version="1.0" encoding="UTF-8"?><ns:Documents xmlns:ns="http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01" xmlns:qp="http://fsrar.ru/WEGAIS/QueryParameters" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="1.0"><ns:Owner><ns:FSRAR_ID>{}</ns:FSRAR_ID></ns:Owner><ns:Document><ns:QueryNATTN><qp:Parameters><qp:Parameter><qp:Name>КОД</qp:Name><qp:Value>{}</qp:Value></qp:Parameter></qp:Parameters></ns:QueryNATTN></ns:Document></ns:Documents>'.format(self.fsrarid, self.fsrarid)
        self.place_in_queue("POST", "/opt/in/QueryNATTN", req_text)
        with dbase.db.mutex:
            dbase.db.set_system("req_nattn_data", int(time.time()))

    def queryttn(self, ttn):
        #self.print_text("запрос ттн {}".format(ttn))
        req_text = '<?xml version="1.0" encoding="UTF-8"?><ns:Documents xmlns:ns="http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01" xmlns:qp="http://fsrar.ru/WEGAIS/QueryParameters" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="1.0"><ns:Owner><ns:FSRAR_ID>{}</ns:FSRAR_ID></ns:Owner><ns:Document><ns:QueryResendDoc><qp:Parameters><qp:Parameter><qp:Name>WBREGID</qp:Name><qp:Value>{}</qp:Value></qp:Parameter></qp:Parameters></ns:QueryResendDoc></ns:Document></ns:Documents>'.format(self.fsrarid, ttn)
        self.place_in_queue("POST", "/opt/in/QueryResendDoc", req_text)
        with dbase.db.mutex:
            dbase.db.set_system("req_ttn_data", int(time.time()))

    def confirm_ttn_v1(self, pid, ttn):
        #self.print_text("запрос ттн {}".format(ttn))
        req_text = '<?xml version="1.0" encoding="UTF-8"?><ns:Documents xmlns:ns="http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01" xmlns:oref="http://fsrar.ru/WEGAIS/ClientRef" xmlns:pref="http://fsrar.ru/WEGAIS/ProductRef" xmlns:wa="http://fsrar.ru/WEGAIS/ActTTNSingle" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="1.0"><ns:Owner><ns:FSRAR_ID>{}</ns:FSRAR_ID></ns:Owner><ns:Document><ns:WayBillAct><wa:Header><wa:IsAccept>{}</wa:IsAccept><wa:ACTNUMBER>{}</wa:ACTNUMBER><wa:ActDate>{}</wa:ActDate><wa:WBRegId>{}</wa:WBRegId></wa:Header><wa:Content/></ns:WayBillAct></ns:Document></ns:Documents>'.format(self.fsrarid, "Accepted", str(pid), time.strftime("%Y-%m-%d", time.localtime(time.time())), ttn)
        self.place_in_queue("POST", "/opt/in/WayBillAct", req_text)

    def confirm_ttn_v2(self, pid, ttn):
        #self.print_text("запрос ттн {}".format(ttn))
        req_text = '<?xml version="1.0" encoding="UTF-8"?><ns:Documents xmlns:ns="http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01" xmlns:oref="http://fsrar.ru/WEGAIS/ClientRef" xmlns:pref="http://fsrar.ru/WEGAIS/ProductRef" xmlns:wa="http://fsrar.ru/WEGAIS/ActTTNSingle_v2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="1.0"><ns:Owner><ns:FSRAR_ID>{}</ns:FSRAR_ID></ns:Owner><ns:Document><ns:WayBillAct_v2><wa:Header><wa:IsAccept>{}</wa:IsAccept><wa:ACTNUMBER>{}</wa:ACTNUMBER><wa:ActDate>{}</wa:ActDate><wa:WBRegId>{}</wa:WBRegId></wa:Header><wa:Content/></ns:WayBillAct_v2></ns:Document></ns:Documents>'.format(self.fsrarid, "Accepted", str(pid), time.strftime("%Y-%m-%d", time.localtime(time.time())), ttn)
        self.place_in_queue("POST", "/opt/in/WayBillAct_v2", req_text)

    def confirm_ttn_v3(self, pid, ttn):
        #self.print_text("запрос ттн {}".format(ttn))
        req_text = '<?xml version="1.0" encoding="UTF-8"?><ns:Documents xmlns:ce="http://fsrar.ru/WEGAIS/CommonV3" xmlns:ns="http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01" xmlns:wa="http://fsrar.ru/WEGAIS/ActTTNSingle_v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="1.0"><ns:Owner><ns:FSRAR_ID>{}</ns:FSRAR_ID></ns:Owner><ns:Document><ns:WayBillAct_v3><wa:Header><wa:IsAccept>{}</wa:IsAccept><wa:ACTNUMBER>{}</wa:ACTNUMBER><wa:ActDate>{}</wa:ActDate><wa:WBRegId>{}</wa:WBRegId></wa:Header><wa:Content/></ns:WayBillAct_v3></ns:Document></ns:Documents>'.format(self.fsrarid, "Accepted", str(pid), time.strftime("%Y-%m-%d", time.localtime(time.time())), ttn)
        self.place_in_queue("POST", "/opt/in/WayBillAct_v3", req_text)

    def do_confirm_ttn(self):
        count = 0
        with dbase.db.mutex:
            rows = dbase.db.get_nattn4()
        for row in rows:
            rowd = dict(row)
            if rowd["doc1"] == 1:
                self.confirm_ttn_v1(rowd["pid"], rowd["ttn"])
            elif rowd["doc1"] == 2:
                self.confirm_ttn_v2(rowd["pid"], rowd["ttn"])
            elif rowd["doc1"] == 3:
                self.confirm_ttn_v3(rowd["pid"], rowd["ttn"])
            with dbase.db.mutex:
                dbase.db.set_nattn(rowd["pid"], "pod_req", 1)
            count += 1
        # if count > 0:
        #     self.print_text("Отправлено подтверждений {}".format(count))


rq = Req()

if __name__ == '__main__':
    a = rq.diagnosis()
    print(a)

