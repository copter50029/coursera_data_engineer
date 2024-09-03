SELECT
	D.year,
	C.country,
	AVG(f.amount) AS totalsales
FROM
	"FactSales" f
JOIN
	"DimDate" D ON f.dateid = D.dateid
JOIN
	"DimCountry" C ON f.countryid = C.countryid
GROUP BY CUBE (D.year,C.country)
ORDER BY
	D.year ASC,
	C.country