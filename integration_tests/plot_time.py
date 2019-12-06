#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def plot_time(runtime, row_number):
    sns.set()
    for phase, time in runtime.items():
        dir = {phase:time, 'rows':row_number}
        df = pd.DataFrame(data = dir)
        # plot each phase versus number of rows 
        sns.lineplot(x='rows', y= phase, data=df)
        plt.xlabel('Number of rows')
        plt.ylabel('Runtime(s)')
        plt.title('Runtime versus Filesize on'+ phase)
    plt.show()

