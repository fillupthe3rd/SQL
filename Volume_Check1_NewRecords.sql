use CAIDataWarehouse
go

select dc.ClientParentNameShort
    , dpr.Product
	, Grouped = 
	case dpr.Product
		when 'Dental' then 'Dental'
		when 'Workers Comp' then 'WC'
		else 'Medical'
	end
    , dd.DateMonthID 
	, dd.DateWeekID
    , dd.DateDay
	, dstc.CategoryDesc
    , sum(fc.ClaimCount) Claims
    , sum(fc.CMAllowed) Charges
            
from FactClaim fc
    join DimDate dd on fc.dimdatereceivedkey = dd.dimdatekey
    join dimclient dc on fc.dimclientkey = dc.dimclientkey
    join dimclaimeligible dce on fc.dimclaimeligiblekey = dce.dimclaimeligiblekey
    join dimservicetypecategory dstc on fc.dimservicetypecategorykey = dstc.dimservicetypecategorykey
    join dimproduct dpr on fc.dimproductkey = dpr.dimproductkey
    join dimclaimtype dct on fc.dimclaimtypekey = dct.dimclaimtypekey
    
where 
    fc.CMID in 
	(
		select fc1.CMID
		from FactClaim fc1
		where fc1.DimClaimEligibleKey = 1
			and fc1.CMID > 13006445
		order by fc1.CMID desc offset 0 rows
	)

group by 
    dc.ClientParentNameShort
    , dpr.Product
    , dd.DateMonthID
	, dd.DateWeekID  
    , dd.DateDay
	, dstc.CategoryDesc
