Makes the search for uniprot ids and such much faster.

i assume indexing protein/gene/pathway/etc would help

```
import arango

class ArangoGraphStore:
    def __init__(self, url: str, username: str, password: str):
        self.url = url
        self.username = username
        self.password = password
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = arango.ArangoClient(self.url)
        return self._client
    
    def spoke(self):
        return self.get_db("spokev6")
    
    def get_db(self, db_name: str):
        return self.client.db(db_name, username=self.username, password=self.password)

    def aql(self,  query: str, db_name: str):
        return self.get_db(db_name).aql(query)
    
```

```
from plrag.datastores.arango import ArangoGraphStore
# read the .env file
import os
from dotenv import load_dotenv

load_dotenv("../.env")

# Initialize graph store
graph_store = ArangoGraphStore(url=os.getenv('SPOKE_DATABASE_URL'), 
                               username=os.getenv("SPOKE_DATABASE_USERNAME"), 
                               password=os.getenv("SPOKE_DATABASE_PASSWORD"),
                               request_timeout=300)
# Create a persistent index on properties.identifier
graph_store.spoke().collection("Nodes").add_index(
    type="persistent",
    fields=["properties.identifier","labels"], 
    unique=False  # Set to True if identifiers are unique
)
```
