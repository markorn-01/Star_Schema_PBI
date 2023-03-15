use OJT;

create table DimDistributor
(
	DISTRIBUTOR_KEY int identity(0,1),
	BU nvarchar(50),
	"Location Level 1" nvarchar(50),
	"Location Level 2" nvarchar(50),
	BU_ORDER int,
	LOCATION_ORDER int
);

with cte_distributor(BU, "Location level 1", "Location level 2", duplicate, BU_ORDER, LOCATION_ORDER)
as (select
	BU,
	GEO,
	Province2,
	row_number() over(partition by BU, GEO, Province2 order by BU) as cnt,
	case
		when BU = 'An Phat PC' then 0
		when BU = 'CellphoneS' then 1
		else 2
	end,
	case 
		when GEO = N'Miền Bắc 1' then 0
		when GEO = N'Miền Bắc 2' then 1
		when GEO = N'Miền Trung' then 2
		when GEO = N'Đông Nam Bộ' then 3
		when GEO = N'Tây Nam Bộ' then 4
		when GEO = N'HN' then 5
		else 6
	end
	from results
)

insert into DimDistributor(BU, "Location Level 1", "Location Level 2", BU_ORDER, LOCATION_ORDER) 	
select BU, 
		"Location level 1", 
		"Location level 2", 
		BU_ORDER, 
		LOCATION_ORDER
from cte_distributor cted
where cted.duplicate = 1;


create table DimProduct
(
	PRODUCT_KEY int identity(0,1),
	Product nvarchar(50),
	"Brand Level 1" nvarchar(50),
	"Brand Level 2" nvarchar(50),
	"Price Class" nvarchar(50),
	PRICE_CLASS_ORDER int
);

with cte_product(Product, "Brand Level 1", "Brand Level 2", duplicate, "Price Class", PRICE_CLASS_ORDER)
as (select
	PRODUCT,
	Brand_Lv1,
	Brand_Lv2,
	row_number() over(partition by Product, "Brand_lv1", "Brand_Lv2", "price_class" order by product) as cnt,
	Price_Class,
	case 
		when Price_Class = '<5M' then 0
		when Price_Class = '5M - 8M' then 1
		when Price_Class = '<8M' then 2
		when Price_Class = '8M - 10M' then 3
		when Price_Class = '10M - 12M' then 4
		when Price_Class = '12M - 14M' then 5
		when Price_Class = '14M - 16M' then 6
		when Price_Class = '12M - 14M' then 7
		when Price_Class = '16M - 20M' then 8
		when Price_Class = '20M - 25M' then 9
		else 10
	end 
	from results
)
insert into DimProduct(Product, "Brand Level 1", "Brand Level 2", "Price Class", PRICE_CLASS_ORDER) 	
select
	Product, "Brand Level 1", "Brand Level 2", "Price Class", PRICE_CLASS_ORDER
from cte_product ctep
where ctep.duplicate = 1;
create table DimDate 
(
		DATE_KEY nvarchar(50),
		Date datetime,
		Month nvarchar(50),
		Year int,
		Weekday nvarchar(50),
		"Day of Month" int,
		"Weekday No" int,
		"Month of Year" date,
		Quarter int,
		"Week of Month" int,
		"Week of Year" int,
		"Month No" int
);

drop table dimdate;

DECLARE @@StartDate DATE = '20170101',
		@@todate date = '20201231',
		@@days int = 0,
		@@day_diff int,
		@@date date
SET		@@day_diff = datediff(dy, @@startdate,@@todate);
declare @tmp int;
set @@days = 0;

while @@days <= @@day_diff
begin
		SET @@date = DATEADD(dd, @@days, @@StartDate);
		SET @@days = @@days + 1;
		insert into DimDate(
				DATE_KEY,
				Date,
				Month,
				Year,
				Weekday,
				"Day of Month",
				"Weekday No",
				"Month of Year",
				Quarter,
				"Week of Month",
				"Week of Year",
				"Month No"
		)
		values(
			FORMAT(@@date, 'yyyyMMdd'),
			@@date,
			datename(month,@@date),
			DATEPART(year, @@date),
			DATENAME(dw, @@date),
			DAY(@@date),
			DATEPART(dw, @@date),
			DATEADD(DAY, 1, EOMONTH(@@date, -1)),
			DATEPART(q, @@date),
			datediff(ww,datediff(d,0,dateadd(m,datediff(m,7,@@date),0))/7*7,dateadd(d,-1,@@date))+1,
			DATEPART(wk, @@date),
			DATEPART(m, @@date)
		)
end;

create table FactSales
(
	DATE_KEY nvarchar(50),
	DISTRIBUTOR_KEY int,
	PRODUCT_KEY int,
	QUANTITY int,
	REVENUE float
);


create table randomdate(
    rdate date,
    idx int identity(0,1)
)

declare @start_date date = '01/01/2017';
declare @end_date date = '12/31/2020';
declare @n int = (select count(*) from results); 
insert into randomdate(rdate)
select top (@n) dateadd(day, abs(checksum(newid())) % datediff(day, @start_date, @end_date), @start_date) as rdate
from sys.all_columns ac1
cross join sys.all_columns ac2;

alter table results add rdate nvarchar(50); 
alter table factsales add idx int identity(0,1);
update rs set rs.rdate = ra.rdate
from results rs
join randomdate ra on rs.idx = ra.idx;

insert into factsales(DATE_KEY)
select FORMAT(cast(rdate as date), 'yyyyMMdd')
from results;

update fa set fa.distributor_key = ddi.distributor_key
from factsales fa
join results rs on fa.idx = rs.idx
join DimDistributor ddi on rs.bu = ddi.BU and rs.geo = ddi.[Location Level 1] and rs.Province2 = ddi.[Location Level 2];

update fa set fa.product_key = dpr.product_key
from FactSales fa
join results rs on fa.idx = rs.idx
join DimProduct dpr on rs.product = dpr.Product and rs.Brand_Lv1 = dpr.[Brand Level 1] and rs.Brand_Lv2 = dpr.[Brand Level 2] and rs.Price_Class = dpr.[Price Class];

update fa set fa.revenue =rs.REVENUE, fa.quantity = rs.qty
from FactSales fa
join  results rs on rs.idx = fa.idx;

