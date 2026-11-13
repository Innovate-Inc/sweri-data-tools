---

```sql
DROP TABLE IF EXISTS
staging.treatment_index_5_year_stats_fy;

CREATE TABLE 
staging.treatment_index_5_year_stats_fy
AS
SELECT * FROM
staging.treatment_index
     WHERE treatment_date BETWEEN '2020-10-01' AND '2025-09-30'
	 and status = 'Completed'
	 and identifier_database in ('IFPRS', 'NFPORS', 'FACTS Hazardous Fuels', 'FACTS Common Attributes');
```

```sql
DROP TABLE IF EXISTS
staging.treatment_index_5_year_stats_cy;

CREATE TABLE 
staging.treatment_index_5_year_stats_cy
AS
SELECT * FROM
staging.treatment_index
     WHERE treatment_date BETWEEN '2021-01-01' AND '2025-12-31'
	 and status = 'Completed'
	 and identifier_database in ('IFPRS', 'NFPORS', 'FACTS Hazardous Fuels', 'FACTS Common Attributes');
```

IIJA Totals by Agency FY 2021-2025

```sql
SELECT 
	agency,
	count(*),
	(SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres,
	COALESCE(SUM(total_cost),0) AS cost

FROM
	staging.treatment_index_5_year_stats_fy

 WHERE fund_source = 'BIL'
 GROUP BY agency
 ORDER BY count DESC;
```

IIJA Totals by twig_category FY 2021-2025

```sql
     SELECT 
	 twig_category,
	 count(*),
	(SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres,
	COALESCE(SUM(total_cost),0) AS cost

FROM
	staging.treatment_index_5_year_stats_fy

 WHERE fund_source = 'BIL'
	 GROUP BY twig_category
	 ORDER BY count DESC;
```

Acres Treated by TWIG Treatment Category - US FY 2021-2025

```sql
     SELECT 
	 twig_category, 
	(SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres

FROM
	staging.treatment_index_5_year_stats_fy
	
	 GROUP BY twig_category
	 ORDER BY acres DESC;
```

Acres Treated by fund_source FY 2021-2025

```sql
     SELECT 
	 fund_source,
	(SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres

FROM
	staging.treatment_index_5_year_stats_fy
	
	 GROUP BY fund_source
	 ORDER BY acres DESC;
```

```sql
     SELECT 
	 fund_code,
	(SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres

FROM
	staging.treatment_index_5_year_stats_fy
	where fund_source = 'Other'
	 GROUP BY fund_code
	 ORDER BY acres DESC;
```

Total Cost by Fund Source FY 2021-2025

---

```sql
     SELECT 
	 fund_source,
	 COALESCE(sum(total_cost),0) AS cost
	
FROM
	staging.treatment_index_5_year_stats_fy
	
	 GROUP BY fund_source
	 ORDER BY cost DESC;
```

```sql
     SELECT 
	 fund_code,
	 COALESCE(sum(total_cost),0) AS cost

FROM
	staging.treatment_index_5_year_stats_fy
	where fund_source = 'Other'
	 GROUP BY fund_code
	 ORDER BY cost DESC;

```

Acres by Agency FY 2021-2025 (treatment index agency field)

```sql
     SELECT 
	 agency,
	 	(SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres
	
FROM
	staging.treatment_index_5_year_stats_fy
	
	 GROUP BY agency
	 ORDER BY acres DESC;
```

### Treatment Index Intersections Continued

- [x]  pairwise intersect treatment index with federal lands
- [x]  calculate areas per federal land with overlap included (completed treatments only fy 21-25)

Note: intersect federal lands first, ti second so agency is referring to fl agency

Treatment Acres by Agency FY 2021-2025 (federal lands ownership intersect)

```sql
SELECT
agency, 
SUM((ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS acres -- Total burned area in acres

FROM
staging.treatment_index_fy_fed_lands_intersect

GROUP BY agency
ORDER BY acres DESC;
```

- [x]  dissolve that layer on the federal lands agency field
- [x]  calculate the area of each of those dissolved shapes to get area without overlap per federal land

!image.png

```sql
SELECT 
agency, 
(ST_Area(ST_Transform(shape, 5070)))/ 4046.8564224 AS acres -- Total treated
FROM
staging.treatment_index_fy_fed_lands_dissolve
ORDER BY acres DESC;
```

for percentage calculations

```sql
select
agency,
SUM((ST_Area(ST_Transform(shape, 5070)))/ 4046.8564224) AS acres
from 
staging.usa_federal_lands_wgs84
group by agency
order by acres desc

```

## Wildfire Processing

Number of fires by agency FY 2021-2025

- [ ]  Manual step : create staging.recent_fires table from https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters/FeatureServer/0
- [ ]  Where : poly_polygondatetime BETWEEN DATE '2020-10-01' AND DATE '2025-09-30'

---

```sql
SELECT 
    fl.agency,
    COUNT(distinct(poly_irwinid)) AS fire_count  -- Count unique fires
FROM 
    staging.recent_fires_8_24 fires
JOIN 
    staging.usa_federal_lands_wgs84 fl
ON 
    ST_Intersects(
        fl.shape,  -- Federal lands shape (in WGS84)
        fires.shape  
    )
GROUP BY 
    fl.agency
ORDER BY
    fire_count DESC;
```

- [x]  Union all fires for analysis with federal lands using pairwise intersect
    
    !image.png
    
    Total acres burned by land ownership FY 2021-2025
    
    staging.recent_fires_fy_2017_2025_federal_lands_intersection is the output from 
    

```sql
SELECT
agency, 
    (SUM(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224) AS total_acres_burned -- Total burned area in acres

FROM
staging.recent_fires_fy_fed_lands_intersect
GROUP BY agency
ORDER BY total_acres_burned DESC;
```

### Pro steps

- [x]  dissolve that layer on the agency field

!image.png

- [x]  get area of dissolved geoms

Total acres burned by land ownership FY 2021-2025 (no overlap counted)

```sql
SELECT
agency, 
(ST_Area(ST_Transform(shape, 5070))) / 4046.8564224 AS total_acres_burned -- Total burned area in acres

FROM
staging.recent_fires_fy_fed_lands_dissolve
ORDER BY total_acres_burned DESC;
```