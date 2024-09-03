CREATE MATERIALIZED VIEW total_sales_per_country AS
SELECT
	C.country,
	SUM(f.amount) AS totalsales
FROM
	"FactSales" f
JOIN
	"DimCountry" C ON f.countryid = C.countryid
GROUP BY
	C.country