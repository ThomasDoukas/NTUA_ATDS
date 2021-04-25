#!/usr/bin/env python
# coding: utf-8

# In[55]:


import numpy as np
import re
import matplotlib.pyplot as plt 
import matplotlib

def get_time(string):
    d = string.split()
    res = re.split(" |m|s",d[1])
    return float(res[0])*60+float(res[1])

# create list of times in secs

mylist=[]
f = open("times.txt", "r")
lines = f.read().splitlines()
for line in lines :
    if len(line.split())>1 and line.split()[0] == "real" :
        mylist.append(np.round(get_time(line),1))



# In[56]:


labels = ['q1', 'q2', 'q3', 'q4', 'q5']

rdd = mylist[0:5]
sql_csv = mylist[5:10]
sql_parq = mylist[10:15]
x = np.arange(len(labels))  # the label locations

width = 0.25  # the width of the bars

fig, ax = plt.subplots()
fig.set_size_inches(12, 8)

rects1 = ax.bar(x - width, rdd, width, label='RDD')
rects2 = ax.bar(x , sql_csv, width, label='SQL-CSV')
rects3 = ax.bar(x + width, sql_parq, width, label='SQL-PARQ')


# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time (sec)')
ax.set_title('Times for each query ')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

fig.tight_layout()

plt.savefig("Barplots/barplots_queries.jpg")
plt.show()


# In[57]:


fig, ax = plt.subplots()
fig.set_size_inches(8, 6)
x= 1
width = 0.05  # the width of the bars

rects1 = ax.bar(x-1.3,height = mylist[15], label='broadcast_join')
rects2 = ax.bar(x, height = mylist[16] , label='library_join')
rects3 = ax.bar(x+1.3,height = mylist[17], label='repartition_join')


# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time (sec)')
ax.set_title('Times for each join type ')

ax.legend()

plt.savefig("Barplots/barplots_rdd_joins.jpg")


# In[58]:


fig, ax = plt.subplots()
fig.set_size_inches(8, 6)
x= 1

rects1 = ax.bar(x-0.65,height = mylist[18], label='broadcast_join')
rects3 = ax.bar(x+0.65,height = mylist[19], label='sortMerge_join')


# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time (sec)')
ax.set_title('Times  for each join type ')

ax.legend()

plt.savefig("Barplots/barplots_sql_join.jpg")


# In[ ]:




