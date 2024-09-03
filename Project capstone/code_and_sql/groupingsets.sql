SELECT
    country,
    category,
    SUM(F.amount) AS totalsales
FROM
    "FactSales" F
INNER JOIN
    "DimCountry" D ON F.countryid = D.countryid
INNER JOIN
    "DimCategory" C ON F.categoryid = C.categoryid
GROUP BY
    GROUPING SETS(
        (country, category),
        (country),
        (category),
        ()
    )
ORDER BY
    country,
    category;
	