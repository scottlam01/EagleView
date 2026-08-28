from fastapi import FastAPI
from backend.database import get_connection
from fastapi.middleware.cors import CORSMiddleware
from backend.states import STATE_MAP, STATE_NAMES
from backend.scoring import (
  calculate_demand_score,
  calculate_salary_score,
  calculate_cost_score,
  calculate_opportunity_score,
  percentile
)
app = FastAPI()

app.add_middleware(
# Allows frontend (React on localhost:5173) to communicate with 
# this backend API from a different origin (different ports)
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# base route tests to see if running
@app.get("/")
def root():
  return {"message": "server is running"}

# ======= Routes =======
# -- Occupations --
@app.get("/occupations")
def get_jobs(q: str):
  conn = get_connection()
  cur = conn.cursor()

  cur.execute("""
      SELECT DISTINCT occ_code, occ_title
      FROM occupations
      WHERE occ_title ILIKE %s
      ORDER BY occ_title;
  """, (f"%{q}%",))

  results = cur.fetchall()
  cur.close()
  conn.close()

  return [
    {"occ_code": r[0], "occ_title": r[1]}
    for r in results
  ]

# -- Cities --
# cities by area_title
@app.get("/areas")
def get_areas(q: str):
  conn = get_connection()
  cur = conn.cursor()

  # Check if user typed a full state name
  search = q.lower().strip()
  state = STATE_MAP.get(search)

  if state:
      cur.execute("""
          SELECT cbsa_code, area_title
          FROM areas
          WHERE prim_state = %s
          ORDER BY area_title;
      """, (state,))
      results = cur.fetchall()
      cur.close()
      conn.close()
  else:
    cur.execute("""
        SELECT cbsa_code, area_title
        FROM areas
        WHERE area_title ILIKE %s
        LIMIT 10;
    """, (f"%{q}%",))
    results = cur.fetchall()
    cur.close()
    conn.close()

  return [
      {"cbsa_code": r[0], "area_title": r[1]}
      for r in results
  ]

# -- Validate user's input when clicking search --
@app.get("/validate_search")
def validate_search(cbsa_code: str, occ_code: str):
    
  conn = get_connection()
  cur = conn.cursor()

  cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM jobs
            WHERE cbsa_code = %s
            AND occ_code = %s
        );
    """, (cbsa_code, occ_code))

  exists = cur.fetchone()[0]

  cur.close()
  conn.close()

  return exists

# -- Dashboard --
@app.get("/dashboard/{occ_code}/{cbsa_code}")
def get_detail_data(occ_code: str, cbsa_code: int):

  conn = get_connection()
  cur = conn.cursor()

  # 1. Query current CBSA statistics
  cur.execute("""
      SELECT
          o.occ_title,
          a.area_title,
          j.tot_emp,
          j.jobs_1000,
          j.loc_quotient,
          j.h_median,
          j.a_pct25,
          j.a_median,
          j.a_pct75,

          CASE
              WHEN r.rpp_all IS NOT NULL AND r.rpp_all != 0
              THEN j.a_median / (r.rpp_all / 100.0)
              ELSE NULL
          END AS real_salary,

          r.rpp_all,
          r.rpp_housing

      FROM jobs j
      JOIN rpp r
          ON j.cbsa_code = r.cbsa_code
      JOIN occupations o
          ON j.occ_code = o.occ_code
      JOIN areas a
          ON j.cbsa_code = a.cbsa_code

      WHERE j.occ_code = %s
        AND j.cbsa_code = %s;
  """, (occ_code, cbsa_code))

  q1row = cur.fetchone()

  if not q1row:
    cur.close()
    conn.close()
    return {"error": "No data found"}

  # 2. Query all CBSA statistics for charting/scoring
  cur.execute("""
      SELECT
          j.cbsa_code,
          a.area_title,

          j.tot_emp,
          j.jobs_1000,
          j.loc_quotient,

          j.a_median,
          j.a_pct25,
          j.a_pct75,

          r.rpp_all,
          r.rpp_housing

      FROM jobs j

      JOIN areas a
          ON j.cbsa_code = a.cbsa_code

      JOIN rpp r
          ON j.cbsa_code = r.cbsa_code

      WHERE j.occ_code = %s;
  """, (occ_code,))

  q2rows = cur.fetchall()

  data = []

  for row in q2rows:
    data.append({
      "cbsa_code": row[0],
      "area_title": row[1],

      "demand": {
          "tot_emp": row[2],
          "jobs_1000": row[3],
          "loc_quotient": row[4]
      },

      "salary": {
          "a_median": row[5],
          "a_pct25": row[6],
          "a_pct75": row[7]
      },

      "cost": {
          "rpp_all": row[8],
          "rpp_housing": row[9]
      }
    })

  

  # Calculate scores for every CBSA
  data = calculate_demand_score(data)
  data = calculate_salary_score(data)
  data = calculate_cost_score(data)
  data = calculate_opportunity_score(data)

  # Calculate percentiles
  demand_scores = [
      city["demand_score"]
      for city in data
  ]

  salary_scores = [
      city["salary_score"]
      for city in data
  ]

  cost_scores = [
      city["cost_score"]
      for city in data
  ]

  opportunity_scores = [
      city["opportunity_score"]
      for city in data
  ]

  # Adds percentile data to all cbsa_data
  for city in data:
      city["demand_percentile"] = percentile(
          city["demand_score"],
          demand_scores
      )

      city["salary_percentile"] = percentile(
          city["salary_score"],
          salary_scores
      )

      city["cost_percentile"] = percentile(
          city["cost_score"],
          cost_scores
      )

      city["opportunity_percentile"] = percentile(
          city["opportunity_score"],
          opportunity_scores
      )


  # Find selected CBSA scores
  current_scores = next(
      city for city in data
      if city["cbsa_code"] == cbsa_code
  )

  cur.close()
  conn.close()

  return {
    "cur_cbsa": {
        "cbsa_code": cbsa_code,
        "occ_code": occ_code,
        "occ_title": q1row[0],
        "area_title": q1row[1],
        "tot_emp": q1row[2],
        "jobs_1000": q1row[3],
        "loc_quotient": q1row[4],
        "h_median": q1row[5],
        "a_pct25": q1row[6],
        "a_median": q1row[7],
        "a_pct75": q1row[8],
        "real_salary": round(q1row[9], 2) if q1row[9] is not None else None,
        "rpp_all": q1row[10],
        "rpp_housing": q1row[11]
    },

    "scores": {
        "demand_score": current_scores["demand_score"],
        "demand_percentile": current_scores["demand_percentile"],

        "salary_score": current_scores["salary_score"],
        "salary_percentile": current_scores["salary_percentile"],

        "cost_score": current_scores["cost_score"],
        "cost_percentile": current_scores["cost_percentile"],

        "opportunity_score": current_scores["opportunity_score"],
        "opportunity_percentile": current_scores["opportunity_percentile"]
    },

    "all_cbsa": data
  }

# =============================