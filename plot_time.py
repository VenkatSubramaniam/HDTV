#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt

def phase_time(runtime, filetype):
    # get the list of phase and the corresponding time
    phase = runtime.keys()
    time = [runtime[key] for key in phase]
    
    # plot in a graph
    plt.figure()
    plt.plot(phase, time)
    plt.xlabel('Different phases')
    plt.ylabel('Runtime(s)')
    plt.title('Runtime versus each phases when parsing'+filetype)

def size_time(runtime, num_rows, filetype):
    pass

