# knowledge_graph_construction_logistics

##### TO USE Relationship Generator #####
1) Install pandas for pythong using: pip install pandas
2) Change Raw File Path to your desired folder which contains raw CSV Data
3) Change Save File Path to your desired folder to save processed Data
4) Run Py script by pressing F5.

Notes:
Folder with timestamp is created. Inside this folder contains folders named as 'Order Number XXXXX'. These folders contain the processed data with relationships. Delayed_Order.csv contains the que for orders based on Earliest Due Date principals.

##### TO USE NEO4J #####
1) Copy processed data to Import folder on Neo4j | File path can be found via Neo4j
2) Use command querries from Fav Neo4j Commands.txt to import CSV data and make relations

