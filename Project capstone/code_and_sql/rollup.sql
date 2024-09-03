SELECT
	D.year,
	C.country,
	SUM(f.amount) AS totalsales
FROM
	"FactSales" f
JOIN
	"DimDate" D ON f.dateid = D.dateid
JOIN
	"DimCountry" C ON f.countryid = C.countryid
GROUP BY ROLLUP (D.year,C.country)
ORDER BY
	D.year DESC,
	C.country
	