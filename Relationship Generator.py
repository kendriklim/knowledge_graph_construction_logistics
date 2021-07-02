# importing os module
import os
from datetime import datetime
import pandas as pd

def getCSV(file):
    while True:
        try:
            file = input(file +' Data : ')
            Data = pd.read_csv(r'' + 'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/' +  file) #Change File Path
            break
            
        except FileNotFoundError :
            print("")
            print('Wrong File Name')
            print( 'Alternatively change file path')
    return Data

#Retrieve Order Data
#orderData = getCSV('Order')
orderData = pd.read_csv(r'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/Order_Data.csv' ) #Change File Path
#Retrieve Depot Worker Data
#depotWorker = getCSV('Depot Worker')
depotWorker = pd.read_csv(r'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/Depot_Worker_Data.csv' ) #Change File Path

#Retrieve Site Worker Data
#siteWorker = getCSV('Site Worker')
siteWorker = pd.read_csv(r'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/Site_Worker_Data.csv' ) #Change File Path

#Retrieve Transport Worker Data
#transportWorker = getCSV('Transport Worker')
transportWorker = pd.read_csv(r'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/Transport_Worker_Data.csv' ) #Change File Path

#Retrieve Project Data
#projectData = getCSV('Project')
projectData = pd.read_csv(r'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/Project_Data.csv' ) #Change File Path

#Determine Delayed Projects & Retrieve Order Number
delayedProjectNo = []
delayedProject = pd.DataFrame (columns = projectData.columns)
for index, row in projectData.iterrows():
    progress = str(row['progress']).upper()

    if progress == 'DELAYED':
        delayedProject = delayedProject.append(projectData.loc[index])
        delayedProjectNo.append(row['projectNo'])
    else:
        pass

#Seperate orders for delayed projects
delayedOrderNo = []
delayedOrder = pd.DataFrame (columns = orderData.columns)
for i in delayedProjectNo:
    for index, row in orderData.iterrows():
        order_project = row['projectNo']
        if order_project == i:
            delayedOrder = delayedOrder.append(orderData.loc[index]) #Adds row to dataframe
            delayedOrderNo.append(order_project)
        else:
            pass

#Reset Index
delayedProject = delayedProject.reset_index()
delayedOrder = delayedOrder.reset_index()

#Date determiner | Sets Earliest Due Date to the Top
delayedOrder['requiredDate'] = pd.to_datetime(delayedOrder.requiredDate, dayfirst = True)
delayedOrder = delayedOrder.sort_values(by='requiredDate', ascending=True)



#Function to determine worker->order
def workerOrder(reqType, df):
    #Zoom in onto order & determine req level for Loading (Depot)
    reqLvl = orderData.loc[orderData["orderNo"] == orderNo, reqType].values[0]
    #Convert to int
    reqLvl = int(reqLvl)

    df = df.drop(df[df.level < reqLvl].index)
    df['orderNo'] = orderNo
    return df

#Empty Data Frame
df = pd.DataFrame

#DepotWorker->Order
depotAvailable = workerOrder('reqLoading', depotWorker)

#SiteWorker->Order
siteAvailable = workerOrder('reqUnload', siteWorker)

#TransportWorker -> Order
transportAvailable = workerOrder('reqTransport', transportWorker)

depotAvailable1=depotAvailable
siteAvailable1 = siteAvailable
transportAvailable1 = transportAvailable

#Remove unavailable workers 1=unavailable, 0=available
siteAvailable = siteAvailable.drop(siteAvailable[siteAvailable.availability == 1].index)
depotAvailable = depotAvailable.drop(depotAvailable[depotAvailable.availability == 1].index)
transportAvailable = transportAvailable.drop(transportAvailable[transportAvailable.availability == 1].index)



#Function to Determine Matching Shift
shiftList = []
def shiftMatcher( data_1, data_2):
    df_1 = pd.DataFrame(columns= data_1.columns)
    df_2 = pd.DataFrame(columns = data_1.columns)
    
    for index, row in data_1.iterrows():
        Shift_1 = row['shift']
        Index_1 = index
        
        for index, row in data_2.iterrows():
            Shift_2 = row['shift']
            Index_2 = index
            workerID_2 = row['workerID']  #Worker Id to match to same shift workers
            
            if Shift_1 == Shift_2:    
                Row_1 = data_1.loc[Index_1]
                Row_2 = data_2.loc[Index_2]

                shiftList.append(workerID_2)

                
                df_1 = df_1.append(Row_1)
                df_2 = df_2.append(Row_2)


            else:
                pass
    df_1['matchingWorker'] = shiftList
    return df_1

#Generate Relationships
depot_site_relationship = shiftMatcher(depotAvailable, siteAvailable)
#Clear List
shiftList = []

site_transport_relationship = shiftMatcher(siteAvailable, transportAvailable)
#Clear List
shiftList = []

depot_transport_relationship = shiftMatcher(depotAvailable, transportAvailable)
#Clear List
shiftList = []


######## Create Folder with Timestamp ############
#Get Timestamp
date = str(datetime.now().strftime("%Y_%m_%d-%I-%M-%S_%p"))

#Order Number
orderNo = str(orderNo)

# Directory
directory = orderNo + " Relation Data " + date
  
# Parent Directory path
parent_dir = "E:/Documents/ARTC Internship/Neo4j/Construction Use Case/Relationship CSV" #Change File Path
  
# Path
path = os.path.join(parent_dir, directory)
  
# Create the directory
try:
    os.makedirs(path, exist_ok = True)
    print("Directory '%s' created successfully" % directory)
except OSError as error:
    print("Directory '%s' can not be created" % directory)

folder = parent_dir + '/' + directory

############# SAVE FILES ##################
depotAvailable.to_csv(r'' + folder + '/Depot_Meet_Req.csv', index = False)
siteAvailable.to_csv(r'' + folder + '/Site_Meet_Req.csv', index = False)
transportAvailable.to_csv(r'' + folder + '/Transport_Meet_Req.csv', index = False)
depot_site_relationship.to_csv(r'' + folder + '/Depot_Site_Relation.csv', index = False)
site_transport_relationship.to_csv(r'' + folder + '/Site_Trans_Relation.csv', index = False)
depot_transport_relationship.to_csv(r'' + folder + '/Depot_Trans_Relation.csv', index = False)

#DElete
depotAvailable1.to_csv(r'' + folder + '/Depot_Meet_Req1.csv', index = False)
siteAvailable1.to_csv(r'' + folder + '/Site_Meet_Req1.csv', index = False)
transportAvailable1.to_csv(r'' + folder + '/Transport_Meet_Req1.csv', index = False)
print('Folder Created')
    



