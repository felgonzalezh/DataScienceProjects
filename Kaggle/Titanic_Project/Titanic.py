import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt # this is used for the plot the graph 
import seaborn as sns # used for plot interactive graph.
import pandas_profiling # import pandas profiling

#%matplotlib inline

testset = pd.read_csv("test.csv", sep=",")
df = pd.read_csv("train.csv", sep=",")


#------------Printing data set info------------------------
print("------- training list dimensions: ------- \n",df.shape)
print("-----info about data: ------- \n ")
print(df.info())
print("------- STATISTICAL INFORMATION: ------- \n",df.describe())
df.hist(figsize=(12,8))
#plt.figure()
plt.show()
print("------- CORRELATIONS: ------- \n",df.corr())
fig,ax = plt.subplots(figsize=(8,7))
ax = sns.heatmap(df.corr(), annot=True,linewidths=.5,fmt='.1f')
plt.show()

# ========= STATISTICAL DESCRPTION ================

# ---SURVIVING RATE PLOT---
col = "Survived"
grouped = df[col].value_counts().reset_index() # def[col] takes the columns Survived, value_counts() 
    #counts the no of each value
    #grouped is a matrix: rows: 0,1. Three Columns: 0 or 1, index and survived.
grouped = grouped.rename(columns = {col : "count", "index" : col}) # (col = Survived) so rename columns: Survived to count, and index to Survived 
# The elements can be accessed as a matrix: list["column"][raw]. So, the number of persons who did not survived is grouped['count'][0]
labels = 'Yes', 'No'
sizes = grouped['count']
fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title("Survived")
plt.show()

# ---SEX PLOT---
col = "Sex"
grouped = df[col].value_counts().reset_index()
grouped = grouped.rename(columns = {col : "count", "index" : col}) 
labels = 'male', 'female'
sizes = grouped['count']
fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title("Sex")
plt.show()

# ---SURVIVING RATE vs SEX PLOT ---
x=df
d1=x[x['Survived']==0] # Selected data of persons who did not survived
d2=x[x['Survived']==1] # Selected data of persons who did survived

col='Sex'
v1=d1[col].value_counts().reset_index()
v1=v1.rename(columns={col:'count','index':col})
v1['percent']=v1['count'].apply(lambda x : 100*x/sum(v1['count'])) # An additional column is included into the data
# "percent" and it is filled by the calculation inside apply function
v1=v1.sort_values(col)
v2=d2[col].value_counts().reset_index()
v2=v2.rename(columns={col:'count','index':col})
v2['percent']=v2['count'].apply(lambda x : 100*x/sum(v2['count']))
v2=v2.sort_values(col)
labels = 'female', 'male'
sizes1 = v1['count']
sizes2 = v2['count']
xl = np.arange(len(labels))  # the label locations (its an arrange=[0,1])
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(xl - width/2, sizes1, width, label='0: No')
rects2 = ax.bar(xl + width/2, sizes2, width, label='1: Yes')
ax.set_ylabel('Number')
ax.set_xlabel('Sex')
ax.set_title('Surviving rate female vs male')
ax.set_xticks(xl)
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
plt.show()

# --------------------SURVIVING RATE vs PClass PLOT -----------------------------------
col='Pclass' 
v1=d1[col].value_counts().reset_index() #v1 -> Not survived
v1=v1.rename(columns={col:'count','index':col})
v1['percent']=v1['count'].apply(lambda x : 100*x/sum(v1['count'])) 
v1=v1.sort_values(col) #v2 -> Survived
v2=d2[col].value_counts().reset_index()
v2=v2.rename(columns={col:'count','index':col})
v2['percent']=v2['count'].apply(lambda x : 100*x/sum(v2['count']))
v2=v2.sort_values(col)
labels = 'upper', 'middle', 'lower'
xl = np.arange(len(labels))  # the label locations (its an arrange=[0,1])
width = 0.5  # the width of the bars
sizes1 = v1['count']
sizes2 = v2['count']

fig, ax = plt.subplots()
rects1 = ax.bar(xl, sizes1, width, label='0: No')
rects2 = ax.bar(xl, sizes2, width, bottom=sizes1, label='1: Yes')
ax.set_ylabel('Number')
ax.set_xlabel('Class')
ax.set_title('Surviving rate in Pclass')
ax.set_xticks(xl)
ax.set_xticklabels(labels)
ax.legend()
plt.show()

############################################
# Testing new  function
rects1 = ax.bar(xl - width/2, sizes1, width, label='0: No')
rects2 = ax.bar(xl + width/2, sizes2, width, label='1: Yes')

def autolabel(*Rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    a = []
    b = []
    
    for rects in Rects:       
        
        x = []
        for rect in rects:
            height = rect.get_height()
            x.append(height)
            if(len(Rects) == 1):
                ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
            a = x 
        b = b + x
    c = []
    if(len(Rects) > 1):
        for i in range(len(rects)):
            c.append(a[i]+b[i])
        
        for rects in Rects:
            print(rects)
            
    print(c)    
    
            
#autolabel(rects1)
#autolabel(rects1,rects2)
        
