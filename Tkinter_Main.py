# tkinter (UI)
import tkinter as tk
from tkinter import (Tk,
                     Frame,
                     StringVar,
                     Label,
                     messagebox)
import tkcalendar
# data
# import pandas as pd
# sql
import sqlite3
# util
import os
# SQL
from olist_query import (query_out,
                         max_min_sales_date,
                         seller_query_func,
                         order_query_func)
from threading import Thread

# Reference :
# https://stackoverflow.com/questions/71345245/frame-inside-a-oop-tkinter-frame

# path manage
base:str = os.path.dirname(__file__)
out_path:str = os.path.join(base,"Out")
sqlite_path:str = os.path.join(base, "Dataset", "olist.sqlite")

# Connection
# Disabled check_same_thread; please check reference for any potential issue
# https://stackoverflow.com/questions/48218065/objects-created-in-a-thread-can-only-be-used-in-that-same-thread
connection = sqlite3.connect(sqlite_path, check_same_thread=False)
# connection = sqlite3.connect(sqlite_path, check_same_thread=True)

# initial query for max and min date
with connection as conn:
    min_max_date_df = max_min_sales_date(conn)

# get the first (min) and last (max) date
min_date = min_max_date_df.iloc[0, 0]
max_date = min_max_date_df.iloc[1, 0]

# 1.0 Main App
class Application(Tk):
    def __init__(self):
        super().__init__()  # initiate class
        # 1. Configuration
        self.geometry("250x300") # size of window
        # self.resizable(False, False) # set windows to be fixed
        self.title("Querying") # Title

        # 2. Frame packing
        # Pack frame from subclass into the main window
        # 2.1 title_frame
        self.title_frame = Frame(self)
        self.title_frame.grid(row=0, column=0, columnspan=2, pady=10, padx=10)
        # Instantiate and pack the TitleFrame
        self.title_frame_widget = self.Query_frame(self.title_frame)
        self.title_frame_widget.grid(row=0, column=0)

    # 1.1 Title frame
    class Query_frame(Frame):
        def __init__(self,parent):
            super().__init__(parent)
            self.title_frame = Frame(self)
            self.title_frame.grid(row=0, column=0, columnspan=2, pady=10, padx=10)
            # Instantiate and pack the TitleFrame
            self.title_frame_widget = self.TitleFrame(self.title_frame)
            self.title_frame_widget.grid(row=0, column=0)

            # 2.2 date_frame
            self.date_frame = Frame(self)
            self.date_frame.grid(row=1, column=0, pady=10, padx=10, rowspan=4)
            # Instantiate and pack the DateSelectorFrame
            self.date_selector_widget = self.DateSelectorFrame(self.date_frame)
            self.date_selector_widget.grid(row=0, column=0)

        class TitleFrame(Frame):
            def __init__(self, parent):
                super().__init__(parent)
                self.configure(height=40)
                # Labels strings
                title_label_text = "Select the date range to be queried:"  # Text

                # title
                title = tk.Label(self, text=title_label_text)
                title.grid()

            # 1.2 Date select frame
        class DateSelectorFrame(Frame):
            def __init__(self, parent):
                super().__init__(parent)

                # querying data using
                def query_run():
                    # get date from DateSelect()
                    end_date = self.end_date_selector.get_date()
                    start_date = self.start_date_selector.get_date()
                    print(end_date)
                    print(start_date)

                    # top level for query dialog
                    query_ongoing_dialog = tk.Toplevel()
                    query_ongoing_dialog.geometry("350x250")

                    # main function run
                    def main():
                        try:
                            # query dialog labeling & packing
                            query_ongoing_dialog_label = Label(query_ongoing_dialog, text="Querying")
                            query_ongoing_dialog_label.pack()

                            # Conn 1 querying
                            # with connection as conn_1:
                            #     df_1 = seller_query_func(conn_1)
                                # print(df_1)
                            # Conn 2 querying
                            with connection as conn_2:
                                df_2 = order_query_func(conn_2, start_date, end_date, output_path=out_path,
                                                               data_only=False)
                                # print(df_2)

                            query_ongoing_dialog.after(0, lambda: query_ongoing_dialog_label.
                                                       config(text='Completed'))

                            query_ongoing_dialog.after(2500, query_ongoing_dialog.destroy)

                        except Exception as e:
                            query_ongoing_dialog.after(0, query_ongoing_dialog.destroy)
                            messagebox.showerror('Error', f"An error has occurred:{e}")

                    # Thread start
                    Thread(target=main).start()

                # 1. Text
                self.configure(height=10, width=10)
                # 1.1 Date Start
                start_date_text = ("Start Date \n"
                                   "(yyyy/mm/dd):")
                start_date_label = Label(self, text = start_date_text)
                # 1.2 Date End
                end_date_text = ("End Date \n"
                                 "(yyyy/mm/dd):")
                end_date_label = tk.Label(self, text = end_date_text)
                # 1.3 Text packing
                # start
                start_date_label.grid(row=0, column=0)
                # end
                end_date_label.grid(row=2, column=0)
                # # 2. Selector


                # Start
                self.start_date_selector = tkcalendar.DateEntry(self, mindate=min_date, maxdate=max_date,
                                                                date_pattern="yyyy/mm/dd")
                self.start_date_selector.grid(row=0, column=1)
                self.start_date_selector.set_date(min_date) # set start date to min date

                # End
                self.end_date_selector = tkcalendar.DateEntry(self, mindate=min_date, maxdate=max_date,
                                                              date_pattern  = "yyyy/mm/dd")
                self.end_date_selector.grid(row=2, column=1)
                self.end_date_selector.set_date(max_date) # set end date to max date


                self.button_run = tk.Button(self, text="Run", command=query_run)
                self.button_run.grid(row=3,column=0, columnspan=2)


if __name__ == '__main__':
    app = Application()
    app.mainloop()
