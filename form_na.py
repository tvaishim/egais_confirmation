import tkinter as tk
from tkinter import ttk
import dbase

class FormNATTN:
    def __init__(self, parent):
        self.root = tk.Toplevel()
        self.parent = parent
        self.root.title("Таблица неподтвержденных ТТН")
        self.root.geometry("800x600")
        self.root.resizable(True, True)
        self.set_widgets()
        self.view_records()

    def set_widgets(self):

        self.frame_top = tk.Frame(self.root, height=40, bd=2)
        self.frame_top.pack(side=tk.TOP, fill=tk.X)

        self.frame_bottom = tk.Frame(self.root, height=20, bd=2)
        self.frame_bottom.pack(side=tk.BOTTOM, fill=tk.X)

        self.btn_ok = tk.Button(self.frame_bottom, text="OK", width=15, command=self.btn_ok_click)
        self.btn_ok.pack(side=tk.RIGHT)

        self.tree = ttk.Treeview(self.root,
                                 columns=("pid", "sel", "doc_data", "doc_num", "ttn", "shipper",
                                          "req", "ans", "doc1", "doc2", "pod_req", "pod_t1", "pod_t2"),
                                 height=15, show="headings")
        self.tree.column("pid", width=15, anchor=tk.CENTER)
        self.tree.column("sel", width=10, anchor=tk.CENTER)
        self.tree.column("doc_data", width=80, anchor=tk.CENTER)
        self.tree.column("doc_num", width=80, anchor=tk.CENTER)
        self.tree.column("ttn", width=80, anchor=tk.CENTER)
        self.tree.column("shipper", width=80, anchor=tk.CENTER)
        self.tree.column("req", width=10, anchor=tk.CENTER)
        self.tree.column("ans", width=10, anchor=tk.CENTER)
        self.tree.column("doc1", width=10, anchor=tk.CENTER)
        self.tree.column("doc2", width=10, anchor=tk.CENTER)
        self.tree.column("pod_req", width=10, anchor=tk.CENTER)
        self.tree.column("pod_t1", width=10, anchor=tk.CENTER)
        self.tree.column("pod_t2", width=10, anchor=tk.CENTER)

        self.tree.heading("pid", text="#")
        self.tree.heading("sel", text="выб")
        self.tree.heading("doc_data", text="Дата")
        self.tree.heading("doc_num", text="Номер")
        self.tree.heading("ttn", text="TTN")
        self.tree.heading("shipper", text="Отправитель")
        self.tree.heading("req", text="зап")
        self.tree.heading("ans", text="отв")
        self.tree.heading("doc1", text="док")
        self.tree.heading("doc2", text="под")
        self.tree.heading("pod_req", text="под.отпр")
        self.tree.heading("pod_t1", text="под.т1")
        self.tree.heading("pod_t2", text="под.т2")

        self.scroll = tk.Scrollbar(self.root, command=self.tree.yview)
        self.scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.configure(yscrollcommand=self.scroll.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=1)
        self.tree.bind("<Double-1>", self.tree_dbl_clk)
        self.tree.bind("<Key-space>", self.tree_dbl_clk)

    def tree_dbl_clk(self, event):
        row = self.tree.item(self.tree.selection()[0],"values")
        with dbase.db.mutex:
            if row[1] == "1":
                dbase.db.set_nattn(row[0], "sel", 0)
                self.tree.set(self.tree.selection()[0], 1, "0")
            else:
                dbase.db.set_nattn(row[0], "sel", 1)
                self.tree.set(self.tree.selection()[0], 1, "1")

    def btn_ok_click(self):
        self.root.destroy()

    def view_records(self):
        with dbase.db.mutex:
            data = dbase.db.nattn_show_data()
        [self.tree.delete(i) for i in self.tree.get_children()]
        [self.tree.insert("", "end", values=tuple(row)) for row in data]


if __name__ == '__main__':
    pass
