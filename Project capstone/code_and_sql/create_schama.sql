-- This script was generated by the ERD tool in pgAdmin 4.
-- Please log an issue at https://redmine.postgresql.org/projects/pgadmin4/issues/new if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE IF NOT EXISTS public."softcartDimDate"
(
    dateid integer NOT NULL,
    date date NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    quartername character varying(2) NOT NULL,
    month integer NOT NULL,
    monthname character varying(255) NOT NULL,
    day integer NOT NULL,
    weekday integer NOT NULL,
    weekdayname character varying(255) NOT NULL,
    PRIMARY KEY (dateid)
);

CREATE TABLE IF NOT EXISTS public."softcartDimCategory"
(
    categoryid integer NOT NULL,
    category character varying(255) NOT NULL,
    PRIMARY KEY (categoryid)
);

CREATE TABLE IF NOT EXISTS public."softcartDimItem"
(
    "Itemid" integer NOT NULL,
    "Item" character varying(255) NOT NULL,
    PRIMARY KEY ("Itemid")
);

CREATE TABLE IF NOT EXISTS public."softcartDimCountry"
(
    countryid integer NOT NULL,
    country character varying(255) NOT NULL,
    PRIMARY KEY (countryid)
);

CREATE TABLE IF NOT EXISTS public."softcartFactSales"
(
    "OrderID" integer NOT NULL,
    dateid integer NOT NULL,
    categoryid integer NOT NULL,
    "Itemid" integer NOT NULL,
    countryid integer NOT NULL,
    price integer NOT NULL,
    PRIMARY KEY ("OrderID")
);

ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."softcartDimDate" (dateid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (categoryid)
    REFERENCES public."softcartDimCategory" (categoryid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY ("Itemid")
    REFERENCES public."softcartDimItem" ("Itemid") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES public."softcartDimCountry" (countryid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;