# importing os module
import os
from datetime import datetime
import pandas as pd



###### Function Not Used #####
###### Get User Input on File Path #####
def getCSV(file):
    while True:
        try:
            file = input(file +' Data : ')
            Data = pd.read_csv(r'' + 'E:/Documents/ARTC Internship/Neo4j/Construction Use Case/' +  file)
            break
            
        except FileNotFoundError :
            print("")
            print('Wrong File Name')
            print( 'Alternatively change file path')
    return Data



###### Change File Path ######
#For Raw File Path 
raw_data_path = 'C:/Users/limyh2/Desktop/Git/knowledge_graph_construction_logistics/CSV Data'

#For Save File Path | Parent Directory path
parent_dir = "C:/Users/limyh2/Desktop/Git/knowledge_graph_construction_logistics/Relationship CSV"

#Retrieve Order Data
#orderData = getCSV('Order')
orderData = pd.read_csv(r'' + raw_data_path + '/Order_Data.csv' )
#Retrieve Depot Worker Data
#depotWorker = getCSV('Depot Worker')
depotWorker = pd.read_csv(r'' + raw_data_path + '/Depot_Worker_Data.csv' )

#Retrieve Site Worker Data
#siteWorker = getCSV('Site Worker')
siteWorker = pd.read_csv(r'' + raw_data_path + '/Site_Worker_Data.csv' )

#Retrieve Transport Worker Data
#transportWorker = getCSV('Transport Worker')
transportWorker = pd.read_csv(r'' + raw_data_path + '/Transport_Worker_Data.csv' )

#Retrieve Project Data
#projectData = getCSV('Project')
projectData = pd.read_csv(r'' + raw_data_path + '/Project_Data.csv' )

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

count = 1
count_list = []
for index, row in delayedOrder.iterrows():
    count_list.append(count)
    count = count + 1

delayedOrder['Que'] = count_list


#Converts to list to save delayed order numbers in a list
orderNo = delayedOrder['orderNo'].tolist()



#Function to determine worker->order
def workerOrder(reqType, df, orderNo):
    #Zoom in onto order & determine req level for Loading (Depot)
    reqLvl = orderData.loc[orderData["orderNo"] == orderNo, reqType].values[0]
    #Convert to int
    reqLvl = int(reqLvl)

    df = df.drop(df[df.level < reqLvl].index)
    df['orderNo'] = orderNo
    return df

#Empty Data Frame
df = pd.DataFrame

#Create Folder
#Get Timestamp
date = str(datetime.now().strftime("%Y_%m_%d-%I-%M-%S_%p"))

# Directory
directory = "Relation CSV_" + date
  

    
# Path
upper_path = os.path.join(parent_dir, directory)

try:
    os.makedirs(upper_path, exist_ok = True)
    print("Directory '%s' created successfully" % directory)
except OSError as error:
    print("Directory '%s' can not be created" % directory)

    
#Save delayed Orders
delayedOrder.to_csv(r'' + upper_path + '/Delayed_Order.csv', index = False)

for x in orderNo:
    
    #DepotWorker->Order
    depotMeetReq = workerOrder('reqLoading', depotWorker, x)

    #SiteWorker->Order
    siteMeetReq = workerOrder('reqUnload', siteWorker, x)

    #TransportWorker -> Order
    transportMeetReq = workerOrder('reqTransport', transportWorker, x)


    #Remove unavailable workers 1=unavailable, 0=available
    siteAvailable = siteMeetReq.drop(siteMeetReq[siteMeetReq.availability == 1].index)
    depotAvailable = depotMeetReq.drop(depotMeetReq[depotMeetReq.availability == 1].index)
    transportAvailable = transportMeetReq.drop(transportMeetReq[transportMeetReq.availability == 1].index)



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


    #Order Number
    x = str(x)

    # Directory
    directory = "Order Number " + x 
      
    # Parent Directory path
    parent_dir = upper_path
      
    # Path
    path = os.path.join(parent_dir, directory)
      
    # Create the directory
    try:
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % directory)
    except OSError as error:
        print("Directory '%s' can not be created" % directory)

    print('Folder Created')
    folder = parent_dir + '/' + directory

    ############# SAVE FILES ##################
    depotAvailable.to_csv(r'' + folder + '/Depot_Available.csv', index = False)
    siteAvailable.to_csv(r'' + folder + '/Site_Available.csv', index = False)
    transportAvailable.to_csv(r'' + folder + '/Transport_Available.csv', index = False)
    depot_site_relationship.to_csv(r'' + folder + '/Depot_Site_Relation.csv', index = False)
    site_transport_relationship.to_csv(r'' + folder + '/Site_Trans_Relation.csv', index = False)
    depot_transport_relationship.to_csv(r'' + folder + '/Depot_Trans_Relation.csv', index = False)


    depotMeetReq.to_csv(r'' + folder + '/Depot_Meet_Req.csv', index = False)
    siteMeetReq.to_csv(r'' + folder + '/Site_Meet_Req.csv', index = False)
    transportMeetReq.to_csv(r'' + folder + '/Transport_Meet_Req.csv', index = False)

print('Files Created Successfully')
    

