<div align="center">
<h1>EagleView</h1>
<p>EagleView is a full-stack career intelligence platform for comparing career opportunities across U.S. metropolitan areas.  It uses data from occupation data from BEA and cost data from BLS to create general scores for different geographical regions called CBSAs.  </p>

<a href="eagleview-career.vercel.app">EagleView </a>

</div>

# Motivation
While looking at different careers I was wondering *which areas would be the most optimal for a specific occupation*.  I came across the fact that working a job in one area will vary drastically in salary, cost, and employment so I wanted to show that through this job intelligence platform.  EagleView combines these factors in a single opportunity score allowing for **different geographical regions to be ranked ultimately leading to an informed career decision**.

# Features
- Search occupations and metropolitan areas
- Interactive dashboard with data visualizations
- View salary and wage statistics
- Rank metropolitan areas by opportunity
- Analyze employment demand
- Compare cost of living

# Tech Stack
<table>
  <tr>
    <td><strong>Frontend</strong></td>
    <td>React, TypeScript, Vite, Mantine, Plotly</td>
  </tr>
  <tr>
    <td><strong>Database</strong></td>
    <td>PostgreSQL, Neon</td>
  </tr>
  <tr>
    <td><strong>Data Processing</strong></td>
    <td>Pandas, NumPy</td>
  </tr>
  <tr>
    <td><strong>Backend</strong></td>
    <td>Python, FastAPI</td>
  </tr>
  <tr>
    <td><strong>Deployment</strong></td>
    <td>Vercel, Render</td>
  </tr>
</table>

# How It Works
1. User selects an occupation and metropolitan area
2. EagleView retrieves employment, wage, and cost-of-living data
3. Data is normalized and used to calculate component scores
4. Scores are combined into an overall Opportunity Score
5. Results are displayed through the dashboard and rankings

# Data Sourcing
- RPP data from [bea.gov](https://apps.bea.gov/itable/?ReqID=70&step=1&_gl=1*c14vb7*_ga*MTAxMjgwNTA1Mi4xNzcyODM3MzEy*_ga_J4698JNNFT*czE3NzI4MzczMTIkbzEkZzAkdDE3NzI4MzczMTIkajYwJGwwJGgw#eyJhcHBpZCI6NzAsInN0ZXBzIjpbMSwyOSwyNSwzMSwyNiwyNywzMF0sImRhdGEiOltbIlRhYmxlSWQiLCIxMDQiXSxbIk1ham9yX0FyZWEiLCI1Il0sWyJTdGF0ZSIsWyI1Il1dLFsiQXJlYSIsWyJYWCJdXSxbIlN0YXRpc3RpYyIsWyItMSJdXSxbIlVuaXRfb2ZfbWVhc3VyZSIsIkxldmVscyJdLFsiWWVhciIsWyIyMDIzIl1dLFsiWWVhckJlZ2luIiwiLTEiXSxbIlllYXJfRW5kIiwiLTEiXV19)
- Occupation and Wage statistics from [bls.gov](https://www.bls.gov/oes/tables.htm)

# Scoring Methodology
<h3 align="center">Opportunity Score</h3>

<table align="center" border="0">
  <tr>
    <td align="center">
      <strong>Demand Score</strong><br>
      40%
    </td>
    <td align="center">+</td>
    <td align="center">
      <strong>Salary Score</strong><br>
      40%
    </td>
    <td align="center">+</td>
    <td align="center">
      <strong>Cost Score</strong><br>
      20%
    </td>
    <td align="center">→</td>
    <td align="center">
      <strong>Opportunity Score</strong>
    </td>
  </tr>
</table>

<p align="center">
  <code>(Demand × 0.40) + (Salary × 0.40) + (Cost × 0.20)</code>
</p>

### Score Components

<table align="center" border="0">
  <tr>
    <td width="33%" align="center">
      <strong>Demand Score</strong><br><br>
      Employment opportunity based on total employment, jobs per 1,000 workers, and location quotient.
    </td>
    <td width="33%" align="center">
      <strong>Salary Score</strong><br><br>
      Wage potential based on 25th percentile, median, and 75th percentile wages.
    </td>
    <td width="33%" align="center">
      <strong>Cost Score</strong><br><br>
      Affordability based on overall and housing Regional Price Parities (RPP).
    </td>
  </tr>
</table>

# Engineering Highlights
- Integrated two government datasets with different structures to work together.
- Designed PostgreSQL tables around CBSA identifiers.
- Built a a REST API with FastAPI to serve occupation, metropolitan area, and dashboard data.
- Developed a scoring system that normalizes multiple labor-market metrics.
- Implemented missing-data handling to prevent incomplete regional data from producing invalid scores.
  
# Future Improvements
- Incorporate more recent BLS and BEA data to provide more current career insights.
- Add additional career factors, such as industry growth and projected employment.
- Add historical trends to show how employment, wages, and opportunity scores change over time.
