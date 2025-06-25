# tkinter (UI)
import tkinter as tk
from tkinter import (Tk,
                     Frame)
import tkcalendar
# data
import pandas as pd
# sql
import sqlite3
# util
import os
import query_test

# Reference :
# https://stackoverflow.com/questions/71345245/frame-inside-a-oop-tkinter-frame

# path manage
base:str = os.path.dirname(__file__)
out_path:str = os.path.join(base,"Out")
sqlite_path:str = os.path.join(base, "Dataset", "olist.sqlite")

#
connection = sqlite3.connect(sqlite_path)

# initial query for max and min date
min_max_date_df = query_test.max_min_sales_date(connection)

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
        self.title_frame_widget = self.TitleFrame(self.title_frame)
        self.title_frame_widget.grid(row=0, column=0)

        # 2.2 date_frame
        self.date_frame = Frame(self)
        self.date_frame.grid(row=1, column=0, pady=10, padx=10)
        # Instantiate and pack the DateSelectorFrame
        self.date_selector_widget = self.DateSelectorFrame(self.date_frame)
        self.date_selector_widget.grid(row=0, column=0)

        # 2.3 Selector

        # 2.4 buttons_frame
        self.button_frame = Frame(self)
        self.button_frame.grid(row=2, column=0, pady=10, padx=10)
        # Instantiate and pack the RunQueryFrame
        self.button_frame = self.RunQueryFrame(self.button_frame)
        self.button_frame.grid(row=0, column=0)



    # 1.1 Title frame
    class TitleFrame(Frame):
        def __init__(self, parent):
            super().__init__(parent)

            self.configure(height=40)

            # Labels strings
            title_label_text = "Select the year range to be queried:"  # Text

            # title
            title = tk.Label(self, text=title_label_text)
            title.grid()

    # 1.2 Date select frame
    class DateSelectorFrame(Frame):
        def __init__(self, parent):
            super().__init__(parent)
            # 1. Text
            self.configure(height=10, width=10)
            # 1.1 Date Start
            start_date_text = ("Start Date \n"
                               "(yyyy/mm/dd):")
            start_date_label = tk.Label(self, text = start_date_text)
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

        def get_start_date(self):
            return self.start_date_selector.get_date()

        # @staticmethod
        # def auto_query_date() -> pd.DataFrame:
        #     #
        #     connection_temp = connection


    class RunQueryFrame(Frame):
        def __init__(self, parent):
            super().__init__(parent)
            #
            self.button_run = tk.Button(self, text="Query")
            self.button_run.grid()






    # 1.1 Frame 1


            #




    # def main_frame(self):
    #     root = tk.Frame






app = Application()
app.mainloop()
