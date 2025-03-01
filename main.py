import os
import base64
import json
import uuid

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List

# Run command: uvicorn main:app --host 127.0.0.1 --port 8000

load_dotenv()

# -----------------------------------------------------------------------------
# Environment Variables for Supabase PostgreSQL connection and Storage
# -----------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST")              # e.g., "db.<project_ref>.supabase.co"
DB_PORT = os.getenv("DB_PORT")                 # default PostgreSQL port
DB_NAME = os.getenv("DB_NAME")              # your database name
DB_USER = os.getenv("DB_USER")              # your database user
DB_PASSWORD = os.getenv("DB_PASSWORD")      # your database password

SUPABASE_URL = os.getenv("SUPABASE_URL")      # your Supabase project URL
SUPABASE_KEY = os.getenv("SUPABASE_KEY")      # your Supabase API key
IMAGE_BUCKET = os.getenv("IMAGE_BUCKET") or "vupi-questions-images"  # your storage bucket name

# -----------------------------------------------------------------------------
# Initialize Supabase Storage Client
# -----------------------------------------------------------------------------
from supabase import create_client, Client
supabase_storage: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------------------------------------------------------
# Database Connection and Initialization (using psycopg2)
# -----------------------------------------------------------------------------
def get_db():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options="-c client_encoding=UTF8"
    )
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options="-c client_encoding=UTF8"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            id SERIAL PRIMARY KEY,
            question_id UUID UNIQUE NOT NULL,
            materia TEXT[],
            assunto TEXT[],
            sub_assunto TEXT[],
            faculdade TEXT,
            ano TEXT,
            data JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

init_db()

# -----------------------------------------------------------------------------
# Pydantic Models (for request validation)
# -----------------------------------------------------------------------------
class FilterModel(BaseModel):
    materia: List[str]
    assunto: List[str]
    subAssunto: List[str]
    faculdade: str
    ano: str

class DataItem(BaseModel):
    id: int
    value: str
    type: str

class Submission(BaseModel):
    data: List[DataItem]
    filter: FilterModel

# -----------------------------------------------------------------------------
# FastAPI App Setup
# -----------------------------------------------------------------------------
app = FastAPI()

# -----------------------------------------------------------------------------
# API Endpoint
# -----------------------------------------------------------------------------
@app.post("/questions")
def create_submission(submission: Submission, db=Depends(get_db)):
    question_uuid = str(uuid.uuid4())
    filename = f"{question_uuid}.png"
    
    cursor = db.cursor(cursor_factory=RealDictCursor)
    
    materia=submission.filter.materia,
    assunto=submission.filter.assunto,
    sub_assunto=submission.filter.subAssunto,
    faculdade=submission.filter.faculdade,
    ano=submission.filter.ano,
    
    # Process each data item (images are uploaded to Supabase Storage)
    processed_data = []
    image_counter = 1
    for item in submission.data:
        if item.type.lower() == "image":
            try:
                image_bytes = base64.b64decode(item.value)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error decoding image data: {e}")
            
            # Generate a filename. The naming logic can be adjusted as needed.
            if image_counter == 1:
                filename = f"{question_uuid}_{image_counter}.png"
            else:
                filename = f"{question_uuid}_{image_counter}.png"
            image_counter += 1

            try:
                # Upload the image to Supabase Storage.
                res = supabase_storage.storage.from_(IMAGE_BUCKET).upload(filename, image_bytes)
                # Check for errors in the upload result.
                if not res:
                    raise HTTPException(status_code=500, detail=f"Error uploading image: {res['error']}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error uploading image: {e}")
            
            # Get the public URL of the uploaded image.
            public_url = supabase_storage.storage.from_(IMAGE_BUCKET).get_public_url(filename)
            processed_data.append({"id": item.id, "type": item.type, "value": public_url})
        else:
            processed_data.append({"id": item.id, "type": item.type, "value": item.value})
    
    # Combine the filter data and processed data into one record.
    record = {
        "question_id": question_uuid,
        "materia": materia,
        "assunto": assunto,
        "sub_assunto": sub_assunto,
        "faculdade": faculdade,
        "ano": ano,
        "data": processed_data  # Stored as JSONB in Postgres
    }
    
    # Insert the record into the 'questions' table.
    try:
        insert_query = """
            INSERT INTO questions (question_id, materia, assunto, sub_assunto, faculdade, ano, data)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """
        cursor.execute(insert_query, (
            record["question_id"],
            record["materia"],
            record["assunto"],
            record["sub_assunto"],
            record["faculdade"],
            record["ano"],
            json.dumps(record["data"])
        ))
        db.commit()
        inserted_id = cursor.fetchone()["id"]
        print('record inserted: ', record)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error inserting record: {e}")
    finally:
        cursor.close()
    
    return {"message": "Submission created successfully", "submission_id": inserted_id}
