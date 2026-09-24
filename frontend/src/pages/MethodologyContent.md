# 1. Data
#### Occupational employment and wage data ####
**Source:** U.S. Bureau of Labor Statistics (BLS), *Occupational Employment and Wage Statistics (OEWS)* — [BLS Occupational Employment and Wage Statistics](https://www.bls.gov/oes/tables.htm)

#### Regional price parity data ####
**Source:** U.S. Bureau of Economic Analysis (BEA), *Regional Price Parities: State and Metro Area* — [BEA Regional Price Parities](https://www.bls.gov/oes/tables.htm)


# 2. Preprocessing
Raw occupational employment and wage data from the U.S. Bureau of Labor Statistics (BLS) and Regional Price Parity (RPP) data from the U.S. Bureau of Economic Analysis (BEA) were preprocessed separately before being combined for analysis.

### Occupational Employment and Wage Data
The BLS metropolitan occupational dataset was filtered to include only detailed occupational records rather than broader occupational groups. This allowed specific occupation comparisons.

Suppressed or unavailable values represented by * or # were converted to missing values (NaN). The absence of a reported value does not imply that employment or wages were zero.

Variables unrelated to EagleView's employment, wage, and occupational-demand analysis were removed to reduce the dataset to the fields required by the application. Column names were standardized to lowercase, and the BLS geographic identifier was renamed to cbsa_code to provide a consistent identifier for metropolitan areas across datasets.

Numeric employment, wage, and occupational concentration variables were converted to appropriate numeric data types. Formatting characters such as commas and suppression symbols were removed before conversion, with invalid or unavailable values converted to missing values.

### Regional Price Parity Data
The BEA RPP dataset was filtered to retain two measures used by EagleView: overall Regional Price Parity (RPP) and housing-rent RPP.  Overall RPP represents differences in general price level across metropolitan areas while housing RPP captures differences in housing rental costs.

The selected RPP measures were reshaped so that overall RPP and housing RPP were represented as separate variables for each metropolitan area. The BEA geographic identifier was renamed to cbsa_code and converted to a numeric type to maintain consistency with the BLS dataset.


# 3. Data Processing & Normalization
The processed BLS and BEA datasets were combined using cbsa_code, which identifies the metropolitan area. This integration allows EagleView to associate employment and wage statistics with the corresponding regional price levels.

The resulting dataset provides the foundation for calculating EagleView's Salary Score, **Demand Score, Cost Score, and Opportunity Score**, as well as the statistics and visualizations presented in the dashboard.

### Normalization
The variables used to calculate EagleView's scores have different units and ranges.  Because these values cannot be directly combined, EagleView uses min-max normalization to transform each metric to a common 0–100 scale.

$$
Normalized(x) =
100 \times \frac{x - x_{min}}{x_{max} - x_{min}}
$$

For each metric, the minimum and maximum values are calculated. Each metropolitan area's value is then normalized using these bounds. A value of **0** represents the lowest observed value, while a value of **100** represents the highest observed value.

If a metric is unavailable for a metropolitan area, its normalized value is treated as missing rather than as zero. If all metropolitan areas have the same value for a metric, the normalized value is assigned a score of **100**, indicating no relative difference among the markets.


# 4. Score Methodology
### Opportunity Score
The **Opportunity Score** is a composite measure that evaluates how attractive a career market is. It combines **Salary, Demand, and Cost** into a single score reported on a 0–100 scale. The Opportunity Score is calculated as:

$$
Opportunity\ Score =
0.40(\text{Salary Score})
+
0.35(\text{Demand Score})
+
0.25(\text{Cost Score})
$$

Each Salary, Demand, and Cost component is normalized to a common 0–100 scale before being combined. The weights reflect the relative importance assigned to each factor in evaluating it’s Opportunity Score.

### Salary Score
The Salary Score measures the relative earning potential of an occupation across metropolitan areas and is reported on a 0–100 scale. The score incorporates three measures from the U.S. Bureau of Labor Statistics (BLS): the median annual wage (A_MEDIAN), 25th percentile wage (A_PCT25), and 75th percentile wage (A_PCT75).

Similarly to Opportunity Score, each wage measure is normalized to allow comparison across metropolitan areas and then combined using weights:

$$
Salary\ Score =
0.50(\text{norm}(A\_MEDIAN))
+
0.25(\text{norm}(A\_PCT25))
+
0.25(\text{norm}(A\_PCT75))
$$

The median wage receives 50% of the weighting because it represents the typical earnings level for the occupation. The 25th and 75th percentile wages each receive 25%, providing additional information about the lower and upper portions of the occupation's wage distribution. This approach prevents the score from relying solely on the median while still giving the typical wage the greatest influence.

A higher Salary Score indicates that an occupation has relatively higher wages compared with the same occupation in other metropolitan areas.

### Demand Score
The **Demand Score** measures the relative strength and prevalence of an occupation within metropolitan areas and is reported on a 0–100 scale. The score incorporates three measures from the U.S. Bureau of Labor Statistics (BLS): total employment (TOT_EMP), jobs per 1,000 workers (JOBS_1000), and the location quotient (LOC_QUOTIENT).

Each measure captures a different aspect of occupational demand. The values are normalized to allow comparison across metropolitan areas and then combined using an equal weighting:

$$
Demand\ Score =
\frac{
\text{norm}(TOT\_EMP)
+
\text{norm}(JOBS\_{1000})
+
\text{norm}(LOC\_QUOTIENT)
}{3}
$$

The three measures are **equally weighted** so that Demand Score reflects a combination of the labor market's overall size, the occupation's prevalence within that market, and its relative concentration. Using all three measures reduces reliance on any single indicator of demand.

A higher Demand Score indicates that an occupation has **relatively stronger employment demand and representation compared with the same occupation in other metropolitan areas.**

### Cost Score
The Cost Score measures the relative affordability of a metropolitan area and is reported on a 0–100 scale. The score incorporates overall cost of living and housing costs, using Regional Price Parities (RPPs) from the U.S. Bureau of Economic Analysis (BEA).

Both measures are normalized to allow comparison across metropolitan areas and combined using an equal weighting:

$$
Cost\ Score =
1 -
\frac{
\text{norm}(Cost\_of\_Living)
+
\text{norm}(Housing\_Cost)
}{2}
$$

Because higher costs represent lower affordability, the average normalized cost is subtracted from 1. This reverses the scale so that **higher Cost Scores represent more affordable metropolitan areas**, while lower scores represent more expensive markets.
A higher Cost Score indicates that a metropolitan area is **relatively more affordable compared with other metropolitan areas.**


# 5. Dashboard Statistics
The dashboard provides five headline statistics summarizing the selected occupation and metropolitan area: **Median Salary, Real Salary, Total Employment, Job Density, and Cost Index**. Together, these metrics provide an overview of earning potential, purchasing power, employment size, occupational prevalence, and regional price levels. Real Salary adjusts the median salary for regional price differences using the metropolitan area's overall RPP, while Cost Index uses an RPP value of 100 as the national price-level benchmark.

*Definitions and interpretations for these statistics are also provided through the interactive tooltips throughout the dashboard.*


# 6. Salary vs. Demand Chart Methodology
The **Salary vs. Demand Chart** displays metropolitan areas according to their Demand Score and Salary Score. Each metropolitan area is represented as a bubble, while the currently selected area is represented as a star. **Bubble size** represents total employment in the selected occupation, providing a visual indication of the relative size of each occupation's labor market. **Bubble color** represents Opportunity Score, allowing users to identify markets with relatively lower or higher overall opportunity.


# 7. Ranking Methodology
The **Ranking Chart** compares metropolitan areas based on their demand, salary, and cost scores for the selected occupation.  Metropolitan areas are ordered from highest to lowest scores, with the highest and lowest scoring markets highlighted alongside the user's selected metropolitan area. 


# 8. Interpretation / Limitations
### Interpretation
EagleView's scores are intended to provide a relative comparison of metropolitan areas for a selected occupation.  The scores should be interpreted in relation to other metropolitan areas rather than as absolute measures of job quality or economic opportunity. The dashboard's supporting statistics and visualizations provide additional context for interpreting these scores.
### Limitations
EagleView's Opportunity Score is a comparative analytical measure and **not a prediction of employment outcomes**. A high score does not guarantee that an individual will find employment, earn the reported salary, or experience a particular quality of life in a metropolitan area.

Also, BLS occupational statistics describe employment and wage characteristics but do not necessarily represent the number of **current job openings** available in a metropolitan area.  Reported wages differ based on a person’s experience, education, and other factors. 

Regional Price Parities provide a measure of relative price levels but do not capture every individual's personal cost of living.  There are also many factors affecting this.  

The scoring model also does not incorporate factors such as job growth, unemployment, specific job openings, remote-work availability, commute times, employer quality, career advancement opportunities, or individual preferences.

Finally, the results depend on the selected occupation, metropolitan areas included in the dataset, data year, normalization method, and weights assigned to each score. Changes to these assumptions may produce different rankings and Opportunity Scores.
