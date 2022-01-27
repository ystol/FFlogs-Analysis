import tkinter as tk
import pandas as pd

# First create application class


class StaticAssigner(tk.Tk):

    def __init__(self, list_vals: pd.DataFrame, window_title, column1, column2):
        super().__init__()

        # df preparation
        self.search_keys = 'search_keys'
        list_vals[self.search_keys] = list_vals[column1] + " | " + list_vals[column2]


        self.search_column1 = column1
        self.search_column2 = column2
        self.title(window_title)
        self.combined_list = list_vals[self.search_keys]
        self.listbox_df = list_vals
        self.create_widgets()

    # Create main GUI window
    def create_widgets(self):
        self.name_search_var = tk.StringVar()
        self.name_search_var.trace("w", self.update_list)

        self.server_search_var = tk.StringVar()
        self.server_search_var.trace("w", self.update_list)

        self.lbox = tk.Listbox(self, width=50, height=15, selectmode='multiple')
        self.lbox.grid(row=2, column=0, padx=5, pady=5, sticky=tk.W+tk.E, columnspan=2)

        self.name_label = tk.Label(text="Character Name")
        self.name_label.grid(row=0, column=0, sticky=tk.W+tk.E)
        self.name_entry = tk.Entry(self, textvariable=self.name_search_var, width=15)
        self.name_entry.grid(row=1, column=0, padx=5, pady=5)

        self.server_label = tk.Label(text="Server Name")
        self.server_label.grid(row=0, column=1, sticky=tk.W + tk.E)
        self.server_entry = tk.Entry(self, textvariable=self.server_search_var, width=15)
        self.server_entry.grid(row=1, column=1, padx=5, pady=5)

        self.static_label = tk.Label(text="Enter Static Name")
        self.static_label.grid(row=3, column=0, sticky=tk.W+tk.E)
        self.static_input = tk.StringVar()
        self.static_name = tk.Entry(self, textvariable=self.static_input)
        self.static_name.grid(row=4, column=0, padx=5, pady=5)

        self.finish_button = tk.Button(self, text='Apply Changes', command=self.close_window)
        self.finish_button.grid(row=4, column=1)

        # Function for updating the list/doing the search.
        # It needs to be called here to populate the listbox.
        self.update_list()

    def update_list(self, *args):
        name_search_term = self.name_search_var.get()
        name_check = self.listbox_df[self.search_column1].str.contains(pat=name_search_term, case=False)
        server_search_term = self.server_search_var.get()
        server_check = self.listbox_df[self.search_column2].str.contains(pat=server_search_term, case=False)
        filtered_list = self.combined_list[name_check & server_check]
        self.lbox.delete(0, 'end')
        for item in filtered_list:
            self.lbox.insert('end', item)

    def close_window(self):
        # for a list of selections
        selected = [self.lbox.get(index) for index in self.lbox.curselection()]
        selection_df = self.listbox_df[self.search_keys].isin(selected)
        self.get_list = self.listbox_df[selection_df]
        self.get_static_input = self.static_input.get()
        self.quit()
