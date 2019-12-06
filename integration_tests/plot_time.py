#!/usr/bin/env python
# coding: utf-8


import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def plot_time(runtime, row_number):
    sns.set()
    for phase, time in runtime.items():
        dict_pair = {"Number of Rows": [], "Runtime": []}
        counter = 0
        for t in time:
            for x in t:
                dict_pair["Runtime"].append(x)
                dict_pair["Number of Rows"].append(row_number[counter])

            counter += 1

        df = pd.DataFrame(data = dict_pair)
        # plot each phase versus number of rows 
        sns.lineplot(x='Number of Rows', y="Runtime", data=df)
        plt.xlabel('Number of rows')
        plt.ylabel('Runtime(s)')
        plt.title('Runtime versus Filesize on'+ phase)
        plt.savefig(f"../util/graph_for_{phase}.png")