-- create tables --

-- parent
CREATE TABLE areas (
	cbsa_code INT PRIMARY KEY,
	area_title TEXT,
	prim_state TEXT
);
-- parent
CREATE TABLE occupations (
	occ_code TEXT PRIMARY KEY,
	occ_title TEXT
);
-- child of areas and occupations
CREATE TABLE jobs (
	cbsa_code INT,
	occ_code TEXT,
	tot_emp INT,
	jobs_1000 FLOAT,
	loc_quotient FLOAT,
	h_mean FLOAT,
	a_mean FLOAT,
	h_pct10 FLOAT,
	h_pct25 FLOAT,
	h_median FLOAT,
	h_pct75 FLOAT,
	h_pct90 FLOAT,
	a_pct10 FLOAT,
	a_pct25 FLOAT,
	a_median FLOAT,
	a_pct75 FLOAT,
	a_pct90 FLOAT,

	PRIMARY KEY (cbsa_code, occ_code),
	FOREIGN KEY (cbsa_code)
		REFERENCES areas(cbsa_code),
	FOREIGN KEY (occ_code) 
		REFERENCES occupations(occ_code)
);
-- no foreign key constraint for rpp.cbsa_code -> areas.cbsa_code 
-- some CBSA codes may not exist in the areas table and vice versa
CREATE TABLE rpp (
	cbsa_code INT PRIMARY KEY,
	rpp_all FLOAT,
	rpp_housing FLOAT
);

