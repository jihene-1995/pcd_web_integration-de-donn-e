from py2neo import neo4j, cypher
graph_db = neo4j.GraphDatabaseService('http://localhost:7474/db/data/')

# Create a Session
session = cypher.Session('http://localhost:7474')
# Create a transaction
tx = session.create_transaction()

# Write your query, and then include it in the transaction
with open("path", "r") as f :
    qs= f.read()
tx.append(qs)
results = tx.commit()
