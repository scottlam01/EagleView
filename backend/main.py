from fastapi import FastAPI
from backend.database import get_connection

app = FastAPI()

# base route tests to see if running
@app.get("/")
def root():
  return {"message": "server is running"}

# ======= Create routes =======
# -- Initial search --
@app.get("/occupations")
def get_jobs():
  conn = get_connection()
  cur = conn.cursor()
  
  cur.execute("SELECT occ_code, occ_title FROM occupations;")
  rows = cur.fetchall()

  cur.close()
  conn.close()

  # convert to JSON
  result = []
  for row in rows:
    result.append({
      "occ_code": row[0],
      "occ_title": row[1]
    })

  return result

# TODO - Map Data
# /map-data 
# input: occ_code
# ouput: (cbsa_code, Total Jobs: tot_emp, Job Concentration: loc_quotient, Median Annual Salary: a_median, Job Score)
@app.get("/map-data ")
def get_map_data():
  conn = get_connection()
  cur = conn.cursor()
  cur.execute("SELECT cbsa_code, tot_emp, loc_quotient, a_median FROM jobs;")

# TODO - Detailed View
# /cbsa/{id}
# input: cbsa_code


# =============================