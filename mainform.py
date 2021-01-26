import tkinter as tk
import time
import req
import dbase
import form_na


class MainForm():
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("ЕГАИС подтверждение")
        self.root.geometry(f"600x400+100+100")
        self.root.resizable()
        self.top_frame_draw()
        self.bottom_frame_draw()
        self.set_button_states()
        self.repaint_form()

    def print_text(self, text):
        self.text.insert(tk.END, text)

    def repaint_form(self):
        if req.rq.utm_online:
            self.root.title("ЕГАИС подтверждение {} [+]".format(req.rq.fsrarid))
        else:
            self.root.title("ЕГАИС подтверждение [-]")
        self.print_text(req.rq.message)
        req.rq.message = ""
        self.set_button_states()
        self.root.after(500, self.repaint_form)

    def top_frame_draw(self):
        self.top_frame = tk.Frame(self.root, height=80, bd=2)
        self.top_frame.pack(side=tk.TOP, fill=tk.X, ipadx=5, ipady=5)

        self.btn_1 = tk.Button(self.top_frame, text="Проверка\nсвязи с УТМ", command=self.btn_1_click)
        self.btn_1.pack(side=tk.LEFT, padx=5)

        self.btn_2 = tk.Button(self.top_frame, text="Запрос\nнеподтвержденных", command=self.btn_2_click)
        self.btn_2.pack(side=tk.LEFT, padx=5)

        self.btn_3 = tk.Button(self.top_frame, text="Запрос\nответов", command=self.btn_3_click)
        self.btn_3.pack(side=tk.LEFT, padx=5)

        self.btn_4 = tk.Button(self.top_frame, text="Таблица\nNATTN", command=self.btn_4_click)
        self.btn_4.pack(side=tk.LEFT, padx=5)

        self.btn_5 = tk.Button(self.top_frame, text="Подтвердить\nдокументы", command=self.btn_5_click)
        self.btn_5.pack(side=tk.LEFT, padx=5)

    def btn_1_click(self):
        req.rq.diagnosis()

    def btn_2_click(self):
        system_data = dbase.db.get_system()
        if (time.time() - system_data["req_nattn_data"]) > 12 * 60 * 60:
            req.rq.querynattn()
        else:
            self.print_text("Запрос неподтвержденных допускается делать не чаще 12 часов\n")
            self.print_text("Последний запрос был сделан {}\n".format(time.strftime("%d.%m.%Y %X", time.localtime(system_data["req_nattn_data"]))))

    def btn_3_click(self):
        req.rq.req_utm_inner()

    def btn_4_click(self):
        self.print_text(dbase.db.nattn_report())
        form_na.FormNATTN(self)

    def btn_5_click(self):
        req.rq.do_confirm_ttn()

    def bottom_frame_draw(self):
        self.bottom_frame = tk.Frame(self.root, height=250, bd=2)
        self.bottom_frame.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=1)

        self.text = tk.Text(self.bottom_frame, wrap=tk.WORD)
        self.text.bind("<Key>", lambda e: "break")
        self.scroll = tk.Scrollbar(self.bottom_frame, command=self.text.yview)
        self.scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.text.config(yscrollcommand=self.scroll.set)
        self.text.pack(side=tk.TOP, fill=tk.BOTH, expand=1)

    def set_button_states(self):

        if req.rq.utm_online:
            self.btn_2.config(state=tk.NORMAL)
            self.btn_3.config(state=tk.NORMAL)
        else:
            self.btn_2.config(state=tk.DISABLED)
            self.btn_3.config(state=tk.DISABLED)


if __name__ == '__main__':
    pass
