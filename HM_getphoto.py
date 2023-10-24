'''
File to load AGN from DES database using SQL queries

Based on OzDES_getPhoto/OzDES_photoDownload.py from Janie Hoorman:
    https://github.com/jhoormann/OzDES_getPhoto/blob/master/OzDES_photoDownload.py

-HM 25/10
'''

#---------------------------------
# Imports & Checks
import numpy as np
import pandas as pd
import os
import warnings
import sys

try:
    import easyaccess as ea
    DEBUG = False
except:
    DEBUG = True
    warnings.warn("easyacces not available. Running in debug mode")
#---------------------------------

# Inputs and output location
try:
    filename = sys.argv[1]
except:
    filename = "./good_agn.csv"
try:
    outloc = sys.argv[2]
    if outloc[:2]!="./": outloc = "./"+outloc
    if outloc[-1]!="/": outloc+="/"
except:
    outloc = filename.replace(".csv","_out/")

# Selection tolerances & data to acquire
RAtol, DECtol = 0.00027, 0.00027
to_select = ["FILENAME", "RA", "DEC"]

#---------------------------------
# Load input csv of targets
print("Loading targets from %s..." %filename)

RM_names = np.loadtxt(filename, delimiter = ',', skiprows = 1,
                      dtype={
                          'names':('ozdes_id', 'RA', 'DEC'),
                          'formats':(str, float, float)
                          })
RM_names = pd.read_csv(filename).to_numpy()
nsources = RM_names.shape[0]

print("Done")
print("-"*76)

#---------------------------------
print("Safety checking save location %s..." %outloc)

if not os.path.exists(outloc):
    os.makedirs(outloc)
    print("\t Folder %s created" %outloc)

print("Done")
print("-"*76)


#---------------------------------

print("Attempting to connect to DES database")
if not DEBUG:
    connection = ea.connect()
else:
    print("\t Skipping due to debug mode")

print("Done")
print("-"*76)

#---------------------------------

query_unf = "select " +\
        ", ".join(to_select) +\
        " where ra between {0} and {1} and dec between {2} and {3}"

#---------------------------------

tick = 50
while nsources//tick<10:
    tick//=2
    if tick==1: break
print("="*(tick+1))

i = 0
for name, RA, DEC in zip(RM_names[:,0],RM_names[:,1],RM_names[:,2]):

    # Generate SQL query
    query = query_unf.format(RA-RAtol, RA+DECtol, DEC-DECtol, DEC+DECtol)

    # Determine file save
    savename = outloc + str(i) + "_" + name + '.tab'

    # Get and save SQL data
    if not DEBUG:
        connection.query_and_save(query, savename)
    else:
        np.savetxt(X = np.array([query]), fname = savename, delimiter=",", fmt="%s")
    
    #Progress bar
    if i%(nsources//tick)==0 and i!=0 and i!=nsources: print("#", end="")
    i+=1

print("")
print("="*(tick+1))
print("Done")

#---------------------------------
