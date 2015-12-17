"""
Copyright (C) 2015 The HDF Group

WARNING: This program is still under construction and only for quick demo. 

==========================================
One-class SVM with non-linear kernel (RBF)
==========================================

An example using a one-class SVM for novelty detection.

`One-class SVM` is an unsupervised
algorithm that learns a decision function for novelty detection:
classifying new data as similar or different to the training set.

This program is adapted from scikit-learn one-class SVM example
to identify outliers in GSSTF NCEP3 Sea Surface Temperature data
near South America equator. 

The goal is to identify El-Nino like event [1]. 

The below is the to-do list:

1) Run multiple classifiers and ensemble the results. 
2) Run clustering on the years that are detected as outliers.

Plot generation is added for quick inspection of learning algorithm's
behavior.

Author: Hyo-Kyung Lee (hyoklee@hdfgroup.org)
Last Update: 2015/12/17

"""
print(__doc__)
import sys
import os
import h5py
import time
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager
from sklearn import svm

# Modify the below.
file_path = '/scr/GSSTF_NCEP.3/GSSTF_NCEP.3.concat.1x72x144.gzip9.h5'
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.1x72x144.gzip9.h5'
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.25x20x20.gzip9.h5'
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.7850x1x1.gzip9.h5'
file_name = os.path.basename(file_path)
h5path = '/HDFEOS/GRIDS/NCEP/Data Fields/SST'
X_outliers = []
X_outliers2 = []
vals = []
vals2 = []
# Put the file name list on the same directory.
with open('ncep3_list.txt') as f:
    content = f.readlines()
    
years = []    
for i in range(len(content)):
    print(content[i][15:-11])
    years.append(content[i][15:-11])
# print years    
start_time = time.time()


# Pacific region near South America.
lat = 360 # equator
lon = 300 # -105 longitude = (+ -180 (* 0.25 300))
lon2 = 290 
with h5py.File(file_path, 'r') as f:
    
    dset = f[h5path]

    for i in range(7850):
        val = dset[i, lat, lon]
        val2 = dset[i, lat, lon2]        
        vals.append(val)
        vals2.append(val2)        
        X_outliers.append([val])
        X_outliers2.append([val2])
end_time = time.time()

# Generate train data.
# You can use the first 80% data as training.
# X_train = X_outliers[0:6279]
ind = np.random.random_integers(0, 7849, 6300)
X_train = []
for i in ind:
    X_train.append(X_outliers[i])
# print X_train

# Generate test data.
# You can use the rest 20% data as testing.
# X_test = X_outliers[6280:7850]
X_test = []
ind2 = np.random.random_integers(0, 7849, 1570)
for j in ind2:
    X_test.append(X_outliers[j])


# Fit the model.
# You can try different nu and gamma like nu = 0.1.
clf = svm.OneClassSVM(nu=0.01, kernel="rbf", gamma=0.1)
clf.fit(X_train)
y_pred_train = clf.predict(X_train)
y_pred_test = clf.predict(X_test)

# This should be changed to other location to be more meaningful.
y_pred_outliers = clf.predict(X_outliers)
y_pred_outliers2 = clf.predict(X_outliers2)
n_error_train = y_pred_train[y_pred_train == -1].size
n_error_test = y_pred_test[y_pred_test == -1].size
n_error_outliers = y_pred_outliers[y_pred_outliers == -1].size
n_error_outliers2 = y_pred_outliers2[y_pred_outliers2 == -1].size
index = np.where(y_pred_outliers == -1)[0]
index2 = np.where(y_pred_outliers2 == -1)[0]

for i in index2:
    print(X_outliers2[i])
    print(content[i])

print(n_error_train)
print(n_error_test)
print(n_error_outliers)
print(n_error_outliers2)

t = np.arange(0, 7850)

plt.title("El Nino Detection")
plt.plot(t, vals)
year = years[0]
for i in index2:
    plt.scatter(i, vals2[i], c='red')
    if year != years[i]:
        plt.text(i, vals2[i]+0.5, years[i])
        year = years[i]
plt.ylabel('SST (deg-C)')
plt.xlabel('time index')
fig = plt.gcf()
# plt.show()
fig.savefig("one_svm.png")

# References
# [1] http://ggweather.com/enso/oni.htm
