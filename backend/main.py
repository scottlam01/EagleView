from fastapi import FastAPI
from backend.database import get_connection

app = FastAPI()

# base route tests to see if running
@app.get("/")
def root():
  return {"message": "server is running"}

# ======= Routes =======
# -- Initial search --
@app.get("/occupations")
def get_jobs():
  conn = get_connection()
  cur = conn.cursor()
  
  cur.execute("""
              SELECT occ_code, occ_title
              FROM occupations
              WHERE LOWER(occ_title) LIKE LOWER(%s)
              LIMIT 20;""")
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

# -- Map Data --
# input: occ_code
# ouput: (cbsa_code, tot_emp, loc_quotient, a_median, Job Score)
@app.get("/map/{occ_code}")
def get_map_data(occ_code: str):
  conn = get_connection()
  cur = conn.cursor()

  cur.execute("""
    WITH filtered AS (
      SELECT *
      FROM jobs
      WHERE occ_code = %s
    ),

    base AS (
      SELECT
        f.cbsa_code,
        f.tot_emp,
        f.jobs_1000,
        f.loc_quotient,
        f.a_median,
        r.rpp_all,
        r.rpp_housing,

        -- demand normalization
        (f.tot_emp - MIN(f.tot_emp) OVER()) / NULLIF(MAX(f.tot_emp) OVER() - MIN(f.tot_emp) OVER(),0) AS norm_tot_emp,
        (f.jobs_1000 - MIN(f.jobs_1000) OVER()) / NULLIF(MAX(f.jobs_1000) OVER() - MIN(f.jobs_1000) OVER(),0) AS norm_jobs_1000,
        (f.loc_quotient - MIN(f.loc_quotient) OVER()) / NULLIF(MAX(f.loc_quotient) OVER() - MIN(f.loc_quotient) OVER(),0) AS norm_lq,

        -- salary normalization
        (f.a_median - MIN(f.a_median) OVER()) / NULLIF(MAX(f.a_median) OVER() - MIN(f.a_median) OVER(),0) AS norm_salary,

        -- cost normalization
        (r.rpp_all - MIN(r.rpp_all) OVER()) / NULLIF(MAX(r.rpp_all) OVER() - MIN(r.rpp_all) OVER(),0) AS norm_cost,
        (r.rpp_housing - MIN(r.rpp_housing) OVER()) / NULLIF(MAX(r.rpp_housing) OVER() - MIN(r.rpp_housing) OVER(),0) AS norm_housing

      FROM filtered f
      JOIN rpp r ON f.cbsa_code = r.cbsa_code
    ),

    scores AS (
      SELECT
        cbsa_code,
        tot_emp,
        loc_quotient,
        a_median,

        -- demand score
        ((norm_tot_emp + norm_jobs_1000 + norm_lq) / 3) * 100 AS demand_score,

        -- salary score
        norm_salary * 100 AS salary_score,

        -- cost score (inverted)
        (1 - ((norm_cost + norm_housing) / 2)) * 100 AS cost_score,

        norm_tot_emp,
        norm_jobs_1000,
        norm_lq,
        norm_salary,
        norm_cost,
        norm_housing

      FROM base
    )

    SELECT
      cbsa_code,
      tot_emp,
      loc_quotient,
      a_median,

      CASE 
        WHEN 
          norm_tot_emp IS NOT NULL AND
          norm_jobs_1000 IS NOT NULL AND
          norm_lq IS NOT NULL AND
          norm_salary IS NOT NULL AND
          norm_cost IS NOT NULL AND
          norm_housing IS NOT NULL
        THEN
          (0.4 * salary_score +
           0.35 * demand_score +
           0.25 * cost_score)
        ELSE NULL
      END AS job_score

    FROM scores
    ORDER BY job_score DESC NULLS LAST;
    """, (occ_code,))

  rows = cur.fetchall()

  cur.close()
  conn.close()

  return [
      {
          "cbsa_code": r[0],
          "tot_emp": r[1],
          "loc_quotient": r[2],
          "a_median": r[3],
          "job_score": round(r[4], 2) if r[4] is not None else None
      }
      for r in rows
  ]

# -- Detailed View --
# input: occ_code, cbsa_code
# output: 
# tot_emp, jobs_1000, loc_quotient, h_median, a_pct25, a_median, a_pct75, a_median/rpp, 
# rpp_all, x% [above or below] average of all rpp_all), rpp_housing, y% [above or below] average of all rpp_housing)
@app.get("/details/{occ_code}/{cbsa_code}")
def get_detail_data(occ_code: str, cbsa_code: int):
  conn = get_connection()
  cur = conn.cursor()
  cur.execute("""
    WITH averages AS (
  SELECT
    AVG(rpp_all) AS avg_rpp_all,
    AVG(rpp_housing) AS avg_rpp_housing
  FROM rpp
)

SELECT
  j.tot_emp,
  j.jobs_1000,
  j.loc_quotient,
  j.h_median,
  j.a_pct25,
  j.a_median,
  j.a_pct75,

  CASE 
    WHEN r.rpp_all IS NOT NULL AND r.rpp_all != 0
    THEN j.a_median / r.rpp_all
    ELSE NULL
  END AS real_salary,

  r.rpp_all,

  CASE 
    WHEN a.avg_rpp_all IS NOT NULL AND a.avg_rpp_all != 0
    THEN ((r.rpp_all - a.avg_rpp_all) / a.avg_rpp_all) * 100
    ELSE NULL
  END AS rpp_all_pct_diff,

  r.rpp_housing,

  CASE 
    WHEN a.avg_rpp_housing IS NOT NULL AND a.avg_rpp_housing != 0
    THEN ((r.rpp_housing - a.avg_rpp_housing) / a.avg_rpp_housing) * 100
    ELSE NULL
  END AS rpp_housing_pct_diff

FROM jobs j
JOIN rpp r ON j.cbsa_code = r.cbsa_code
CROSS JOIN averages a

WHERE j.occ_code = %s
  AND j.cbsa_code = %s;
  """, (occ_code, cbsa_code))

  row = cur.fetchone()

  cur.close()
  conn.close()

  if not row:
      return {"error": "No data found"}

  return {
      "tot_emp": row[0],
      "jobs_1000": row[1],
      "loc_quotient": row[2],
      "h_median": row[3],
      "a_pct25": row[4],
      "a_median": row[5],
      "a_pct75": row[6],
      "real_salary": round(row[7], 2) if row[7] is not None else None,
      "rpp_all": row[8],
      "rpp_all_pct_diff": round(row[9], 2) if row[9] is not None else None,
      "rpp_housing": row[10],
      "rpp_housing_pct_diff": round(row[11], 2) if row[11] is not None else None
  }

# =============================