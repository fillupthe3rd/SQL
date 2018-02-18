use CAIDataWarehouse
go 

with cte_daily as
(
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
		, dst.CategoryDesc
        , sum(fc.ClaimCount) Claims
        , sum(fc.CMAllowed) Charges
            
    from FactClaim fc
        join DimDate dd on fc.dimdatereceivedkey = dd.dimdatekey
        join dimclient dc on fc.dimclientkey = dc.dimclientkey
        join dimclaimeligible dce on fc.dimclaimeligiblekey = dce.dimclaimeligiblekey
        join dimdiscountmethod ddm on fc.dimdiscountmethodkey = ddm.dimdiscountmethodkey
        join dimprovider dp on fc.dimproviderkey = dp.dimproviderkey
        join dimservicetypecategory dstc on fc.dimservicetypecategorykey = dstc.dimservicetypecategorykey
        join dimnetwork dn on fc.dimnetworkkey = dn.dimnetworkkey
        join dimproduct dpr on fc.dimproductkey = dpr.dimproductkey
        join dimclaimtype dct on fc.dimclaimtypekey = dct.dimclaimtypekey
        join DimClaimStatus dcs on fc.DimClaimStatusKey = dcs.DimClaimStatusKey
    
    where 
        dce.ClaimEligible = 'Eligible'
            and dd.DateDay between (convert(date, getdate() - 120)) and (convert(date, getdate()))
              
    group by 
        dc.ClientParentNameShort
        , dpr.Product
        , dd.DateMonthID
		, dd.DateWeekID  
        , dd.DateDay
	order by fc.CMID
)
    
select c.ClientParentNameShort
    , c.Product
	, c.grp
    , c.DateMonthID
	, c.DateWeekID
    , c.DateDay
    , c.Claims
    , c.Charges
    , lag(c.Claims, 1) over(partition by c.ClientParentNameShort, c.Product order by c.DateMonthID) Claims_prev
    , lag(c.Charges, 1) over(partition by c.ClientParentNameShort, c.Product order by c.DateMonthID) Charges_prev
    , c.Claims - (lag(c.Claims, 1) over(partition by c.ClientParentNameShort, c.Product order by c.DateMonthID)) as diff_claims
    , c.Charges - (lag(c.Charges, 1) over(partition by c.ClientParentNameShort, c.Product order by c.DateMonthID)) as diff_charges
        
from cte_daily c 
    
;