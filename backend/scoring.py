'''
Scoring.py Creates:
Demand Score
Salary Score
Cost Score
Opportunity Score
'''


'''
Demand Score = avg(
  norm(tot_emp),
  norm(jobs_1000),
  norm(loc_quotient)
)
'''
def calculate_demand_score(cbsa_data):
  # get min and max for each cbsa
  tot_emp_values = get_metric_values(
    cbsa_data,
    "demand",
    "tot_emp"
  )
  min_tot_emp = min(tot_emp_values)
  max_tot_emp = max(tot_emp_values)

  jobs_1000_values = get_metric_values(
    cbsa_data,
    "demand",
    "jobs_1000"
  )
  min_jobs_1000 = min(jobs_1000_values)
  max_jobs_1000 = max(jobs_1000_values)

  loc_q_values = get_metric_values(
    cbsa_data,
    "demand",
    "loc_quotient"
  )
  min_loc_q = min(loc_q_values)
  max_loc_q = max(loc_q_values)

  # normalize and calculate score for every cbsa
  for city in cbsa_data:

    norm_tot_emp = normalize(
        city["demand"]["tot_emp"],
        min_tot_emp,
        max_tot_emp
    )

    norm_jobs_1000 = normalize(
        city["demand"]["jobs_1000"],
        min_jobs_1000,
        max_jobs_1000
    )

    norm_loc_q = normalize(
        city["demand"]["loc_quotient"],
        min_loc_q,
        max_loc_q
    )

    scores = [
        norm_tot_emp,
        norm_jobs_1000,
        norm_loc_q
    ]
    # create a list of scores that only includes not null values
    valid_scores = [
      score for score in scores
      if score is not None
    ]

    if len(valid_scores) > 0:
    # adds demand score to cbsa_data rounded whole
      city["demand_score"] = round(
          sum(valid_scores) / len(valid_scores),
          1
      )
    else:
      city["demand_score"] = None

  return cbsa_data


'''
Salary Score = avg(
  norm(a_median),
  norm(a_pct25),
  norm(a_pct75)
)
'''
def calculate_salary_score(cbsa_data):

  # get min and max for each cbsa
  median_values = get_metric_values(
      cbsa_data,
      "salary",
      "a_median"
  )
  min_median = min(median_values)
  max_median = max(median_values)

  pct25_values = get_metric_values(
      cbsa_data,
      "salary",
      "a_pct25"
  )
  min_pct25 = min(pct25_values)
  max_pct25 = max(pct25_values)

  pct75_values = get_metric_values(
      cbsa_data,
      "salary",
      "a_pct75"
  )
  min_pct75 = min(pct75_values)
  max_pct75 = max(pct75_values)

  # normalize and calculate score for every cbsa
  for city in cbsa_data:

      norm_median = normalize(
          city["salary"]["a_median"],
          min_median,
          max_median
      )

      norm_pct25 = normalize(
          city["salary"]["a_pct25"],
          min_pct25,
          max_pct25
      )

      norm_pct75 = normalize(
          city["salary"]["a_pct75"],
          min_pct75,
          max_pct75
      )

      scores = [
        norm_median,
        norm_pct25,
        norm_pct75
      ]
      # create a list of scores that only includes not null values
      valid_scores = [
          score for score in scores
          if score is not None
      ]

      if len(valid_scores) > 0:
          city["salary_score"] = round(
              sum(valid_scores) / len(valid_scores),
              1
          )
      else:
          city["salary_score"] = None

  return cbsa_data


'''
Cost Score = avg(
  norm(rpp_all),
  norm(rpp_housing)
)
'''
def calculate_cost_score(cbsa_data):
  # Get values for min/max
  rpp_values = get_metric_values(
      cbsa_data,
      "cost",
      "rpp_all"
  )
  min_rpp = min(rpp_values)
  max_rpp = max(rpp_values)

  housing_values = get_metric_values(
      cbsa_data,
      "cost",
      "rpp_housing"
  )
  min_housing = min(housing_values)
  max_housing = max(housing_values)


  for city in cbsa_data:

      norm_rpp = normalize(
          city["cost"]["rpp_all"],
          min_rpp,
          max_rpp
      )

      norm_housing = normalize(
          city["cost"]["rpp_housing"],
          min_housing,
          max_housing
      )

      cost_values = [
          norm_rpp,
          norm_housing
      ]
      # create a list of scores that only includes not null values
      valid_cost_values = [
          value for value in cost_values
          if value is not None
      ]

      if len(valid_cost_values) > 0:
        avg_cost = sum(valid_cost_values) / len(valid_cost_values)

        # Higher cost = lower score, so invert
        city["cost_score"] = round(
            100 - avg_cost,
            1
        )
      else:
        city["cost_score"] = None

  return cbsa_data


'''
-- Weights for opportunity score may change
Opportunity Score =
  (0.4 * Salary) +
  (0.35 * Demand) +
  (0.25 * Cost)
'''
def calculate_opportunity_score(cbsa_data):

  for city in cbsa_data:
      
      scores = [
            (city["demand_score"], 0.35),
            (city["salary_score"], 0.30),
            (city["cost_score"], 0.20)
        ]

      weighted_scores = [
          score * weight
          for score, weight in scores
          if score is not None
      ]

      # adds opportunity score to cbsa_data rounded whole
      if weighted_scores:
          city["opportunity_score"] = round(
              sum(weighted_scores),
              1
          )
      else:
          city["opportunity_score"] = None

  return cbsa_data

# function used to normalize values used in scoring
def normalize(value, minimum, maximum):
  if value is None:
      return None
  if maximum == minimum:
      return 100
  return ((value - minimum) / (maximum - minimum)) * 100


# Returns the percentile rank of a score compared to all scores.
"""
    score: the current CBSA's score
    all_scores: list of scores from all CBSAs
"""
def percentile(score, all_scores):
    
  valid_scores = [
      value for value in all_scores
      if value is not None
  ]

  if score is None or len(valid_scores) == 0:
      return None

  count_below = sum(
      1 for value in valid_scores
      if value < score
  )

  return round(
      (count_below / len(valid_scores)) * 100,
      1
  )

# helper function to extract a metric from every CBSA while ignoring missing values
def get_metric_values(cbsa_data, category, metric):
    return [
        city[category][metric]
        for city in cbsa_data
        if city[category][metric] is not None
    ]