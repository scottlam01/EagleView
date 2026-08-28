# EagleView
A data-driven web application that helps users analyze job opportunities across different locations.

# Data Sourcing
- RPP data from [bea.gov](https://apps.bea.gov/itable/?ReqID=70&step=1&_gl=1*c14vb7*_ga*MTAxMjgwNTA1Mi4xNzcyODM3MzEy*_ga_J4698JNNFT*czE3NzI4MzczMTIkbzEkZzAkdDE3NzI4MzczMTIkajYwJGwwJGgw#eyJhcHBpZCI6NzAsInN0ZXBzIjpbMSwyOSwyNSwzMSwyNiwyNywzMF0sImRhdGEiOltbIlRhYmxlSWQiLCIxMDQiXSxbIk1ham9yX0FyZWEiLCI1Il0sWyJTdGF0ZSIsWyI1Il1dLFsiQXJlYSIsWyJYWCJdXSxbIlN0YXRpc3RpYyIsWyItMSJdXSxbIlVuaXRfb2ZfbWVhc3VyZSIsIkxldmVscyJdLFsiWWVhciIsWyIyMDIzIl1dLFsiWWVhckJlZ2luIiwiLTEiXSxbIlllYXJfRW5kIiwiLTEiXV19)
- Occupation and Wage statistics from [bls.gov](https://www.bls.gov/oes/tables.htm)

# How to set up locally
## Create two terminals, one for backend one for frontend
### 1. Clone repo
`git clone <repo-url>`
### 2. Enter project root
`cd EagleView`
### 3. Backend (terminal 1)
`uvicorn backend.main:app --reload`
### 4. Frontend (terminal 2)
`cd frontend`
`npm install`
`npm run dev`