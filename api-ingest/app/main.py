from fastapi import FastAPI, status, HTTPException
 
# to be able to turn classes into JSONs and return
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# for json.dumps
import json

# for BaseModel
from pydantic import BaseModel
from typing import Optional

from datetime import datetime
from kafka import KafkaProducer, producer

# create class (schema) for the JSON
# timestamp get's ingested as string and then before writing validated
class UserBehavior(BaseModel):
    timestamp: str
    visitorid: int
    event: str
    itemid: int
    transactionid: Optional[int] = None

app = FastAPI()

# base URL
@app.get("/")
async def root():
    return {"message": "Hello World"}

# ingest
@app.post("/events")
async def post_event(event: UserBehavior):
    # logs
    print(f"Received event: {event.event} from visitor {event.visitorid}")
    
    try:
        # parse events back to json from objects
        json_of_event = jsonable_encoder(event)
        # dump the json out as string, produce the string
        produce_kafka_string(json.dumps(json_of_event))
        # encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content=json_of_event, status_code=201)
    
    except ValueError:
        return JSONResponse(content={"error": "Invalid data"}, status_code=400)
    
# query
# @app.get("/analytics/funnel")
# async def get_funnel(start_date: str = None, end_date: str = None):
#     """funnel: view -> addtocart -> transaction"""
#     pipeline = [
#         {"$group": {"_id": "$event", "count": {"$sum": 1}}}
#     ]
#     result = list(db.events.aggregate(pipeline))
#     return {"funnel": result}

# @app.get("/analytics/realtime")
# async def get_realtime():
#     """real time metrics"""
#     pv = db.events.count_documents({"event": "view"})
#     uv = len(db.events.distinct("visitorid"))
#     return {"pv": pv, "uv": uv}

# @app.get("/analytics/trending")
# async def get_trending(limit: int = 10):
#     """top items"""
#     pipeline = [
#         {"$group": {"_id": "$itemid", "views": {"$sum": 1}}},
#         {"$sort": {"views": -1}},
#         {"$limit": limit}
#     ]
#     result = list(db.events.aggregate(pipeline))
#     return {"trending": result}

def produce_kafka_string(json_as_string):
    # Create producer
        producer = KafkaProducer(bootstrap_servers='kafka:9092',acks=1)
        
        # Write the string as bytes because Kafka needs it this way
        producer.send('ingestion-topic', bytes(json_as_string, 'utf-8'))
        producer.flush() 


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)