########################################################
### Replication of Cooper, Gulen, and Schill (2008)
# This code replicates and extends the result of the
# asset growth anomaly using data from Compustat and CRSP
########################################################
########################################################
### STEP 1: Setup                                   
# Set up the working environment 
# and connect to the wrds database
########################################################

# import library
library(tidyverse); library(zoo); library(magrittr);library(lubridate); require(data.table)

# set directory
setwd("C:/") #input your working directory

# connect to wrds
library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  user='yourname',
                  password='yourpassword',
                  dbname='wrds',
                  sslmode='require')


########################################################
### STEP 2: Import and clean data from wrds        
# This step downloads stock price and return data from CRSP
# and accounting data from Compustat, then merge the 
# datasets using ccm
########################################################

# data from Compustat funda database: 
res <- dbSendQuery(wrds,"select GVKEY, FYEAR, DATADATE, AT, NI, PRCC_F, CSHO, CUSIP, DP,
                        SEQ, TXDITC, PSTKRV, PSTKL, PSTK, EPSPI, OIBDP, DLTT, DLC, SALE, SICH,
                        CAPX, XINT, TXT, DVP, DVC, CHE, MIB, CEQ, PPENT, ACT, RE, LCT, TXP
                    from COMP.FUNDA
                    where INDFMT='INDL' and DATAFMT='STD' and CONSOL='C' and POPSRC='D'") 
data.comp <- dbFetch(res, n = -1) # n=-1 denotes no max but retrieve all record

# clean Compustat data: only use data from company that's avaliable for 2 years to avoid backfilling error
data.comp <- data.comp %>%
  arrange(fyear,gvkey) %>% 
  group_by(gvkey) %>% 
  slice(3:n()) %>% # number of years in data
  ungroup
save(data.comp, file = "comp.RData")

# download link table from ccm
res <- dbSendQuery(wrds,"select GVKEY, LPERMNO, LINKDT, LINKENDDT, LINKTYPE, LINKPRIM
                    from crsp.ccmxpf_lnkhist")
data.ccmlink <- dbFetch(res, n = -1) 
save(data.ccmlink, file = "ccm.RData")
load("ccm.RData")
# merge CCM permno to Compustat
data.ccm <-  data.ccmlink %>%
  # use only primary links (from WRDS Merged Compustat/CRSP examples)
  filter(linktype %in% c("LU", "LC")) %>%
  filter(linkprim %in% c("P", "C")) %>%
  merge(data.comp.funda, by="gvkey") %>% # inner join, keep only if permno exists
  mutate(datadate = as.Date(datadate), 
         permno = as.factor(lpermno),
         linkdt = as.Date(linkdt),
         linkenddt = as.Date(linkenddt),
         linktype = factor(linktype, levels=c("LC", "LU")),
         linkprim = factor(linkprim, levels=c("P", "C"))) %>%
  # remove compustat fiscal ends that do not fall within linked period; linkenddt=NA (from .E) means ongoing  
  filter(datadate >= linkdt & (datadate <= linkenddt | is.na(linkenddt))) %>%
  # prioritize linktype, linkprim based on order of preference/primary if duplicate
  arrange(datadate, permno, linktype, linkprim) %>%
  distinct(datadate, permno, .keep_all = TRUE)
save(data.ccm, file = "data.ccm.RData")
rm(data.comp, data.ccmlink)

# data from CRSP msf: trading information
res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, CFACPR, CFACSHR, SHROUT, PRC, RET, RETX, VOL
                   from CRSP.MSF")
crsp.msf <- dbFetch(res, n = -1) 
# data from CRSP mse: exchange code and sic code
res <- dbSendQuery(wrds, "select DATE, PERMNO, SHRCD, EXCHCD, SICCD
                   from CRSP.MSE")
crsp.mse <- dbFetch(res, n = -1)
# merge the CRSP data
crsp.msf <- crsp.msf %>%
  filter(!is.na(prc)) %>%
  mutate(date =as.Date(date))
crsp.mse <- crsp.mse %>%
  filter(!is.na(siccd)) %>%
  mutate(date = as.Date(date))
data.crsp <- crsp.msf %>%
  merge(crsp.mse, by=c("date", "permno"), all=TRUE) %>%
  mutate(permno = as.factor(permno)) %>%
  arrange(date, permno) %>%
  group_by(permno) %>%  
  # fill in NA's with latest available (must sort by Date and group by PERMNO)
  fill(shrcd, exchcd, siccd) %>% 
  ungroup %>%
  filter(!is.na(prc))
rm(crsp.mse,crsp.msf)
save(data.crsp, file = "crsp.RData")

# calculate market cap
load("crsp.RData")
data.crsp <- data.crsp %>%
  # me for each permno
  mutate(meq = shrout * abs(prc)) %>% 
  group_by(date, permco) %>%
  # to calc market cap, merge permnos with same permnco
  mutate(ME = sum(meq)) %>% 
  mutate(ME = ME/1000)  %>%
  arrange(date, permco, desc(meq)) %>%
  group_by(date, permco) %>%
  # keep only permno with largest meq
  slice(1) %>% 
  ungroup

# create an anual file of CRSP
data.crsp <- data.crsp%>% 
  mutate(date = as.Date(date)) %>%
  arrange(date,permno) %>%
  group_by(permno) %>%
  mutate(MV = ifelse(as.numeric(month(date))==6, ME, NA), # market value use the market cap at June
         #ME is used as the denominator of BM ratio, and is the market cap at Dec
         ME = ifelse(as.numeric(month(date))==12, ME, NA),
         # the return from July t to June t+1
         ret.1yr = (lead(ret,1)+1)*(lead(ret,2)+1)*(lead(ret,3)+1)*(lead(ret,4)+1)*
           (lead(ret,5)+1)*(lead(ret,6)+1)*(lead(ret,7)+1)*(lead(ret,8)+1)*
           (lead(ret,9)+1)*(lead(ret,10)+1)*(lead(ret,11)+1)*(lead(ret,12)+1)-1,
         year = as.numeric(year(date)),
         BHRET6 = (ret+1)*(lag(ret,1)+1)*(lag(ret,2)+1)*(lag(ret,3)+1)*(lag(ret,4)+1)*(lag(ret,5)+1)-1,
         # ret.7 to ret.12 is July to Dec ret in year t
         ret.7 = lead(ret,1), 
         ret.8 = lead(ret,2),
         ret.9 = lead(ret,3),
         ret.10 = lead(ret,4),
         ret.11 = lead(ret,5),
         ret.12 = lead(ret,6),
         # ret.1 to ret.6 is Jan to June ret in year t+1
         ret.1 = lead(ret,7), 
         ret.2 = lead(ret,8),
         ret.3 = lead(ret,9),
         ret.4 = lead(ret,10),
         ret.5 = lead(ret,11),
         ret.6 = lead(ret,12),)%>%
  fill(ME)%>% 
  ungroup%>% 
  filter(as.numeric(month(date))==6)%>%
  # follow Fama&French, only use NYSE, AMEX, and NASDAQ stock, and only use ordinary common shares
  filter((shrcd == 10 | shrcd == 11) & (exchcd == 1 | exchcd == 2 | exchcd == 3)) %>%
  select(year,permno,MV,ME,ret.1yr,siccd,exchcd,BHRET6,ret.1,ret.2,ret.3,ret.4,ret.5,
         ret.6,ret.7,ret.8,ret.9,ret.10,ret.11,ret.12)


# merge CRSP data to the merged table, only keep Compustat entries which can be matched to CRSP
df <- data.ccm %>%  
  # accounting variables in Compustat t-1 should be matched to CRSP return July t to June t+1
  mutate(year = fyear+1) %>%   
  merge(data.crsp, ., by=c("year", "permno"), all.x=TRUE) %>%  # keep all CRSP records
  arrange(permno, year, desc(datadate)) %>%
  distinct(permno, year, .keep_all = TRUE) # drop older datadates

# create variables for regression
# the definition of these variables can be found in the CGS (2008) paper
df <- df %>%
  arrange(year,permno) %>%
  group_by(permno) %>%
  # generate varibles  
  mutate(SIC = coalesce(sich, siccd), 
         BE = seq+txditc-coalesce(pstkrv,pstkl,pstk),
         BM = BE/ME,
         A.MV = at/MV,
         EP = epspi/prcc_f,
         ROA = oibdp/at,
         LEVERAGE = (dltt+dlc)/at,
         SALESG = sale/lag(sale)-1,
         BHRET36 = (lag(ret.1yr,1)+1)*(lag(ret.1yr,2)+1)*(lag(ret.1yr,3)+1)-1,
         ASSETG = at/lag(at)-1,
         rank.ASSETG = rank(ASSETG, ties.method = c("average")),
         ASSETG5Y = 0.1*lag(rank.ASSETG,5)+0.2*lag(rank.ASSETG,4)+0.3*lag(rank.ASSETG,3)+0.4*lag(rank.ASSETG,2),
         rank.SALESG = rank(SALESG, ties.method = c("average")),
         SALESG5Y = 0.1*lag(rank.SALESG,5)+0.2*lag(rank.SALESG,4)+0.3*lag(rank.SALESG,3)+0.4*lag(rank.SALESG,2),
         L2ASSETG = lag(ASSETG),
         CE = capx/sale,
         CI = CE/((lag(CE,1)+lag(CE,2)+lag(CE,3))/3),
         CASH.FLOW = (oibdp-(xint+txt+dvp+dvc))/at,
         Leverage = (dltt/(dltt+prcc_f*csho)),
         OA = at-che,
         OL = at-dlc-dltt-mib-pstk-ceq,
         NOA = (OA-OL)/lag(at),
         NOA.A = (OA-OL)/at,
         ACCRUALS = ((act-lag(act)-che+lag(che))-(lct-lag(lct)-dlc+lag(dlc)-txp+lag(txp))-dp)/((at+lag(at))/2),
         ISSUANCE = log((prcc_f*csho)/(lag(prcc_f,3)*lag(csho,3)))-log(BHRET36+1),
         Cashg = (che-lag(che))/lag(at),
         CurAsstg = (act-lag(act)-che+lag(che))/lag(at),
         PPEg = (ppent-lag(ppent))/lag(at),
         Othersg = ASSETG - Cashg - CurAsstg - PPEg,
         REg = (re-lag(re))/lag(at),
         StockFing = (pstk-lag(pstk)+ceq-lag(ceq)+mib-lag(mib)-re+lag(re))/lag(at),
         Debtg = (dlc-lag(dlc))/lag(at),
         OpLiabg = ASSETG - REg - StockFing - Debtg)%>% 
  ungroup 

########################################################
### STEP 3: Add industry classification             
# I use the industry classification from French's website
# and group stocks by their SIC into 48 industires
########################################################

df$industry <- NaN
df$industry.name <- NaN

df$industry[(df$SIC>=0100 & df$SIC<=0199)
            |(df$SIC>=0200 & df$SIC<=0299)
            |(df$SIC>=0700 & df$SIC<=0799)
            |(df$SIC>=0910 & df$SIC<=0919)
            |(df$SIC>=2048 & df$SIC<=2048)] = 1
df$industry.name[df$industry==1] = "Argic"

df$industry[(df$SIC>= 2000 & df$SIC<= 2009)
            |(df$SIC>= 2010 & df$SIC<= 2019)
            |(df$SIC>= 2020 & df$SIC<= 2029)
            |(df$SIC>= 2030 & df$SIC<= 2039)
            |(df$SIC>= 2040 & df$SIC<= 2046)
            |(df$SIC>= 2050 & df$SIC<= 2059)
            |(df$SIC>= 2060 & df$SIC<= 2063)
            |(df$SIC>= 2070 & df$SIC<= 2079)
            |(df$SIC>= 2090 & df$SIC<= 2092)
            |(df$SIC>= 2095 & df$SIC<= 2095)
            |(df$SIC>= 2098 & df$SIC<= 2099)] = 2
df$industry.name[df$industry==2] = "Food"

df$industry[(df$SIC>= 2064 & df$SIC<= 2068)
            |(df$SIC>= 2086 & df$SIC<= 2086)
            |(df$SIC>= 2087 & df$SIC<= 2087)
            |(df$SIC>= 2096 & df$SIC<= 2096)
            |(df$SIC>= 2097 & df$SIC<= 2097)] = 3
df$industry.name[df$industry==3] = "Soda"

df$industry[(df$SIC>= 2080 & df$SIC<= 2080)
            |(df$SIC>= 2082 & df$SIC<= 2082)
            |(df$SIC>= 2083 & df$SIC<= 2083)
            |(df$SIC>= 2084 & df$SIC<= 2084)
            |(df$SIC>= 2085 & df$SIC<= 2085)] = 4
df$industry.name[df$industry==4] = "Beer"

df$industry[(df$SIC>= 2100 & df$SIC<= 2199)] = 5
df$industry.name[df$industry==5] = "Smoke"

df$industry[(df$SIC>= 0920 & df$SIC<= 0999) 
            |(df$SIC>= 3650 & df$SIC<= 3651) 
            |(df$SIC>= 3652 & df$SIC<= 3652)
            |(df$SIC>= 3732 & df$SIC<= 3732)
            |(df$SIC>= 3930 & df$SIC<= 3931)
            |(df$SIC>= 3940 & df$SIC<= 3949)] = 6
df$industry.name[df$industry==6] = 'Toys'

df$industry[(df$SIC>= 7800 & df$SIC<= 7829)
            |(df$SIC>=  7830 & df$SIC<= 7833)
            |(df$SIC>= 7840 & df$SIC<= 7841)
            |(df$SIC>= 7900 & df$SIC<= 7900)
            |(df$SIC>= 7910 & df$SIC<= 7911)
            |(df$SIC>= 7920 & df$SIC<= 7929)
            |(df$SIC>= 7930 & df$SIC<= 7933)
            |(df$SIC>= 7940 & df$SIC<= 7949)
            |(df$SIC>= 7980 & df$SIC<= 7980)
            |(df$SIC>= 7990 & df$SIC<= 7999)] = 7
df$industry.name[df$industry==7] = "Fun"

df$industry[(df$SIC>= 2700 & df$SIC<= 2709)
            |(df$SIC>= 2710 & df$SIC<= 2719)
            |(df$SIC>= 2720 & df$SIC<= 2729)
            |(df$SIC>= 2730 & df$SIC<= 2739)
            |(df$SIC>= 2740 & df$SIC<= 2749)
            |(df$SIC>= 2770 & df$SIC<= 2771)
            |(df$SIC>= 2780 & df$SIC<= 2789)
            |(df$SIC>= 2790 & df$SIC<= 2799)] = 8
df$industry.name[df$industry==8] = "Books"

df$industry[(df$SIC>= 2047 & df$SIC<= 2047)
            |(df$SIC>= 2391 & df$SIC<= 2392)
            |(df$SIC>= 2510 & df$SIC<= 2519)
            |(df$SIC>= 2590 & df$SIC<= 2599)
            |(df$SIC>= 2840 & df$SIC<= 2843)
            |(df$SIC>= 2844 & df$SIC<= 2844)
            |(df$SIC>= 3160 & df$SIC<= 3161)
            |(df$SIC>= 3170 & df$SIC<= 3171)
            |(df$SIC>= 3172 & df$SIC<= 3172)
            |(df$SIC>= 3190 & df$SIC<= 3199)
            |(df$SIC>= 3229 & df$SIC<= 3229)
            |(df$SIC>= 3260 & df$SIC<= 3260)
            |(df$SIC>= 3262 & df$SIC<= 3263)
            |(df$SIC>= 3269 & df$SIC<= 3269)
            |(df$SIC>= 3230 & df$SIC<= 3231)
            |(df$SIC>= 3630 & df$SIC<= 3639)
            |(df$SIC>= 3750 & df$SIC<= 3751)
            |(df$SIC>= 3800 & df$SIC<= 3800)
            |(df$SIC>= 3860 & df$SIC<= 3861)
            |(df$SIC>= 3870 & df$SIC<= 3873)
            |(df$SIC>= 3910 & df$SIC<= 3911)
            |(df$SIC>= 3914 & df$SIC<= 3914)
            |(df$SIC>= 3915 & df$SIC<= 3915)
            |(df$SIC>= 3960 & df$SIC<= 3962)
            |(df$SIC>= 3991 & df$SIC<= 3991)
            |(df$SIC>= 3995 & df$SIC<= 3995)] = 9
df$industry.name[df$industry==9] = "Hshld"

df$industry[(df$SIC>= 2300 & df$SIC<= 2390)
            |(df$SIC>= 3020 & df$SIC<= 3021)
            |(df$SIC>= 3100 & df$SIC<= 3111)
            |(df$SIC>= 3130 & df$SIC<= 3131)
            |(df$SIC>= 3140 & df$SIC<= 3149)
            |(df$SIC>= 3150 & df$SIC<= 3151)
            |(df$SIC>= 3963 & df$SIC<= 3965)] = 10
df$industry.name[df$industry==10] = "Clths"

df$industry[(df$SIC>= 8000 & df$SIC<= 8099)] = 11
df$industry.name[df$industry==11] = "Hlth"

df$industry[(df$SIC>= 3693 & df$SIC<= 3693)
            |(df$SIC>= 3840 & df$SIC<= 3849)
            |(df$SIC>= 3850 & df$SIC<= 3851)] = 12
df$industry.name[df$industry==12] = "MedEq"

df$industry[(df$SIC>=  2830 & df$SIC<= 2830)
            |(df$SIC>= 2831 & df$SIC<= 2831)
            |(df$SIC>= 2833 & df$SIC<= 2833)
            |(df$SIC>= 2834 & df$SIC<= 2834)
            |(df$SIC>= 2835 & df$SIC<= 2835)
            |(df$SIC>= 2836 & df$SIC<= 2836)] = 13
df$industry.name[df$industry==13] = "Drugs"

df$industry[(df$SIC>= 2800 & df$SIC<= 2809)
            |(df$SIC>= 2810 & df$SIC<= 2819)
            |(df$SIC>= 2820 & df$SIC<= 2829)
            |(df$SIC>= 2850 & df$SIC<= 2859)
            |(df$SIC>= 2860 & df$SIC<= 2869)
            |(df$SIC>= 2870 & df$SIC<= 2879)
            |(df$SIC>= 2890 & df$SIC<= 2899)] = 14
df$industry.name[df$industry==14] = "Chems"

df$industry[(df$SIC>=  3031 & df$SIC<= 3031)
            |(df$SIC>= 3041 & df$SIC<= 3041)
            |(df$SIC>= 3050 & df$SIC<= 3053)
            |(df$SIC>= 3060 & df$SIC<= 3069)
            |(df$SIC>= 3070 & df$SIC<= 3079)
            |(df$SIC>= 3080 & df$SIC<= 3089)
            |(df$SIC>= 3090 & df$SIC<= 3099)] = 15
df$industry.name[df$industry==15] = "Rubbr"

df$industry[(df$SIC>= 2200 & df$SIC<= 2269)
            |(df$SIC>= 2270 & df$SIC<= 2279)
            |(df$SIC>= 2280 & df$SIC<= 2284)
            |(df$SIC>= 2290 & df$SIC<= 2295)
            |(df$SIC>= 2297 & df$SIC<= 2297)
            |(df$SIC>= 2298 & df$SIC<= 2298)
            |(df$SIC>= 2299 & df$SIC<= 2299)
            |(df$SIC>= 2393 & df$SIC<= 2395)
            |(df$SIC>= 2397 & df$SIC<= 2399)] = 16
df$industry.name[df$industry==16] = "Txtls"

df$industry[(df$SIC>= 0800 & df$SIC<= 0899)
            |(df$SIC>= 2400 & df$SIC<= 2439)
            |(df$SIC>= 2450 & df$SIC<= 2459)
            |(df$SIC>= 2490 & df$SIC<= 2499)
            |(df$SIC>= 2660 & df$SIC<= 2661)
            |(df$SIC>= 2950 & df$SIC<= 2952)
            |(df$SIC>= 3200 & df$SIC<= 3200)
            |(df$SIC>= 3210 & df$SIC<= 3211)
            |(df$SIC>= 3240 & df$SIC<= 3241)
            |(df$SIC>= 3250 & df$SIC<= 3259)
            |(df$SIC>= 3261 & df$SIC<= 3261)
            |(df$SIC>= 3264 & df$SIC<= 3264)
            |(df$SIC>= 3270 & df$SIC<= 3275)
            |(df$SIC>= 3280 & df$SIC<= 3281)
            |(df$SIC>= 3290 & df$SIC<= 3293)
            |(df$SIC>= 3295 & df$SIC<= 3299)
            |(df$SIC>= 3420 & df$SIC<= 3429)
            |(df$SIC>= 3430 & df$SIC<= 3433)
            |(df$SIC>= 3440 & df$SIC<= 3441)
            |(df$SIC>= 3442 & df$SIC<= 3442)
            |(df$SIC>= 3446 & df$SIC<= 3446)
            |(df$SIC>= 3448 & df$SIC<= 3448)
            |(df$SIC>= 3449 & df$SIC<= 3449)
            |(df$SIC>= 3450 & df$SIC<= 3451)
            |(df$SIC>= 3452 & df$SIC<= 3452)
            |(df$SIC>= 3490 & df$SIC<= 3499)
            |(df$SIC>= 3996 & df$SIC<= 3996)] = 17
df$industry.name[df$industry==17] = "BldMt"

df$industry[(df$SIC>= 1500 & df$SIC<= 1511)
            |(df$SIC>= 1520 & df$SIC<= 1529)
            |(df$SIC>= 1530 & df$SIC<= 1539)
            |(df$SIC>= 1540 & df$SIC<= 1549)
            |(df$SIC>= 1600 & df$SIC<= 1699)
            |(df$SIC>= 1700 & df$SIC<= 1799)] = 18
df$industry.name[df$industry==18] = "Cnstr"

df$industry[(df$SIC>= 3300 & df$SIC<= 3300)
            |(df$SIC>= 3310 & df$SIC<= 3317)
            |(df$SIC>= 3320 & df$SIC<= 3325)
            |(df$SIC>= 3330 & df$SIC<= 3339)
            |(df$SIC>= 3340 & df$SIC<= 3341)
            |(df$SIC>= 3350 & df$SIC<= 3357)
            |(df$SIC>= 3360 & df$SIC<= 3369)
            |(df$SIC>= 3370 & df$SIC<= 3379)
            |(df$SIC>= 3390 & df$SIC<= 3399)] = 19
df$industry.name[df$industry==19] = "Steel"

df$industry[(df$SIC>= 3400 & df$SIC<= 3400)
            |(df$SIC>= 3443 & df$SIC<= 3443)
            |(df$SIC>= 3444 & df$SIC<= 3444)
            |(df$SIC>= 3460 & df$SIC<= 3469)
            |(df$SIC>= 3470 & df$SIC<= 3479)] = 20
df$industry.name[df$industry==20] = "FabPr"

df$industry[(df$SIC>= 3510 & df$SIC<= 3519)
            |(df$SIC>= 3520 & df$SIC<= 3529)
            |(df$SIC>= 3530 & df$SIC<= 3530)
            |(df$SIC>= 3531 & df$SIC<= 3531)
            |(df$SIC>= 3532 & df$SIC<= 3532)
            |(df$SIC>= 3533 & df$SIC<= 3533)
            |(df$SIC>= 3534 & df$SIC<= 3534)
            |(df$SIC>= 3535 & df$SIC<= 3535)
            |(df$SIC>= 3536 & df$SIC<= 3536)
            |(df$SIC>= 3538 & df$SIC<= 3538)
            |(df$SIC>= 3540 & df$SIC<= 3549)
            |(df$SIC>= 3550 & df$SIC<= 3559)
            |(df$SIC>= 3560 & df$SIC<= 3569)
            |(df$SIC>= 3580 & df$SIC<= 3580)
            |(df$SIC>= 3581 & df$SIC<= 3581)
            |(df$SIC>= 3582 & df$SIC<= 3582)
            |(df$SIC>= 3585 & df$SIC<= 3585)
            |(df$SIC>= 3586 & df$SIC<= 3586)
            |(df$SIC>= 3589 & df$SIC<= 3589)
            |(df$SIC>= 3590 & df$SIC<= 3599)] = 21
df$industry.name[df$industry==21] = "Mach"

df$industry[(df$SIC>= 3600 & df$SIC<= 3600)
            |(df$SIC>= 3610 & df$SIC<= 3613)
            |(df$SIC>= 3620 & df$SIC<= 3621)
            |(df$SIC>= 3623 & df$SIC<= 3629)
            |(df$SIC>= 3640 & df$SIC<= 3644)
            |(df$SIC>= 3645 & df$SIC<= 3645) 
            |(df$SIC>= 3646 & df$SIC<= 3646)
            |(df$SIC>= 3648 & df$SIC<= 3649)
            |(df$SIC>= 3660 & df$SIC<= 3660)
            |(df$SIC>= 3690 & df$SIC<= 3690)
            |(df$SIC>= 3691 & df$SIC<= 3692)
            |(df$SIC>= 3699 & df$SIC<= 3699)] = 22
df$industry.name[df$industry==22] = "ElcEq"

df$industry[(df$SIC>=  2296 & df$SIC<= 2296)
            |(df$SIC>= 2396 & df$SIC<= 2396)
            |(df$SIC>= 3010 & df$SIC<= 3011)
            |(df$SIC>= 3537 & df$SIC<= 3537)
            |(df$SIC>= 3647 & df$SIC<= 3647)
            |(df$SIC>= 3694 & df$SIC<= 3694)
            |(df$SIC>= 3700 & df$SIC<= 3700)
            |(df$SIC>= 3710 & df$SIC<= 3710)
            |(df$SIC>= 3711 & df$SIC<= 3711)
            |(df$SIC>= 3713 & df$SIC<= 3713)
            |(df$SIC>= 3714 & df$SIC<= 3714)
            |(df$SIC>= 3715 & df$SIC<= 3715)
            |(df$SIC>= 3716 & df$SIC<= 3716)
            |(df$SIC>= 3792 & df$SIC<= 3792)
            |(df$SIC>= 3790 & df$SIC<= 3791)
            |(df$SIC>= 3799 & df$SIC<= 3799)] = 23
df$industry.name[df$industry==23] = "Autos"

df$industry[(df$SIC>= 3720 & df$SIC<= 3720)
            |(df$SIC>= 3721 & df$SIC<= 3721)
            |(df$SIC>= 3723 & df$SIC<= 3724)
            |(df$SIC>= 3725 & df$SIC<= 3725)
            |(df$SIC>= 3728 & df$SIC<= 3729)] = 24
df$industry.name[df$industry==24] = "Aero"

df$industry[(df$SIC>= 3730 & df$SIC<= 3731)
            |(df$SIC>= 3740 & df$SIC<= 3743)] = 25
df$industry.name[df$industry==25] = "Ships"

df$industry[(df$SIC>= 3760 & df$SIC<= 3769)
            |(df$SIC>= 3795 & df$SIC<= 3795)
            |(df$SIC>= 3480 & df$SIC<= 3489)] = 26
df$industry.name[df$industry==26] = "Guns"

df$industry[(df$SIC>= 1040 & df$SIC<= 1049)] = 27;
df$industry.name[df$industry==27] = "Gold"

df$industry[(df$SIC>= 1000 & df$SIC<= 1009)
            |(df$SIC>= 1010 & df$SIC<= 1019)
            |(df$SIC>= 1020 & df$SIC<= 1029)
            |(df$SIC>= 1030 & df$SIC<= 1039)
            |(df$SIC>= 1050 & df$SIC<= 1059)
            |(df$SIC>= 1060 & df$SIC<= 1069)
            |(df$SIC>= 1070 & df$SIC<= 1079)
            |(df$SIC>= 1080 & df$SIC<= 1089)
            |(df$SIC>= 1090 & df$SIC<= 1099)
            |(df$SIC>= 1100 & df$SIC<= 1119)
            |(df$SIC>= 1400 & df$SIC<= 1499)] = 28
df$industry.name[df$industry==28] = "Mines"

df$industry[(df$SIC>= 1200 & df$SIC<= 1299)] = 29
df$industry.name[df$industry==29] = "Coal"

df$industry[(df$SIC>= 1300 & df$SIC<= 1300)
            |(df$SIC>= 1310 & df$SIC<= 1319)
            |(df$SIC>= 1320 & df$SIC<= 1329)
            |(df$SIC>= 1330 & df$SIC<= 1339)
            |(df$SIC>= 1370 & df$SIC<= 1379)
            |(df$SIC>= 1380 & df$SIC<= 1380)
            |(df$SIC>= 1381 & df$SIC<= 1381)
            |(df$SIC>= 1382 & df$SIC<= 1382)
            |(df$SIC>= 1389 & df$SIC<= 1389)
            |(df$SIC>= 2900 & df$SIC<= 2912)
            |(df$SIC>= 2990 & df$SIC<= 2999)] = 30
df$industry.name[df$industry==30] = "Oil"

df$industry[(df$SIC>= 4900 & df$SIC<= 4900)
            |(df$SIC>= 4910 & df$SIC<= 4911)
            |(df$SIC>= 4920 & df$SIC<= 4922)
            |(df$SIC>= 4923 & df$SIC<= 4923)
            |(df$SIC>= 4924 & df$SIC<= 4925)
            |(df$SIC>= 4930 & df$SIC<= 4931)
            |(df$SIC>= 4932 & df$SIC<= 4932)
            |(df$SIC>= 4939 & df$SIC<= 4939)
            |(df$SIC>= 4940 & df$SIC<= 4942)] = 31
df$industry.name[df$industry==31] = "Util"

df$industry[(df$SIC>= 4800 & df$SIC<= 4800)
            |(df$SIC>= 4810 & df$SIC<= 4813)
            |(df$SIC>= 4820 & df$SIC<= 4822)
            |(df$SIC>= 4830 & df$SIC<= 4839)
            |(df$SIC>= 4840 & df$SIC<= 4841)
            |(df$SIC>= 4880 & df$SIC<= 4889)
            |(df$SIC>= 4890 & df$SIC<= 4890)
            |(df$SIC>= 4891 & df$SIC<= 4891)
            |(df$SIC>= 4892 & df$SIC<= 4892)
            |(df$SIC>= 4899 & df$SIC<= 4899)] = 32
df$industry.name[df$industry==32] = "Telcm"

df$industry[(df$SIC>= 7020 & df$SIC<= 7021)
            |(df$SIC>= 7030 & df$SIC<= 7033)
            |(df$SIC>= 7200 & df$SIC<= 7200)
            |(df$SIC>= 7210 & df$SIC<= 7212)
            |(df$SIC>= 7214 & df$SIC<= 7214)
            |(df$SIC>= 7215 & df$SIC<= 7216)
            |(df$SIC>= 7217 & df$SIC<= 7217)
            |(df$SIC>= 7219 & df$SIC<= 7219)
            |(df$SIC>= 7220 & df$SIC<= 7221)
            |(df$SIC>= 7230 & df$SIC<= 7231)
            |(df$SIC>= 7240 & df$SIC<= 7241)
            |(df$SIC>= 7250 & df$SIC<= 7251)
            |(df$SIC>= 7260 & df$SIC<= 7269)
            |(df$SIC>= 7270 & df$SIC<= 7290)
            |(df$SIC>= 7291 & df$SIC<= 7291)
            |(df$SIC>= 7292 & df$SIC<= 7299)
            |(df$SIC>= 7395 & df$SIC<= 7395)
            |(df$SIC>= 7500 & df$SIC<= 7500)
            |(df$SIC>= 7520 & df$SIC<= 7529)
            |(df$SIC>= 7530 & df$SIC<= 7539)
            |(df$SIC>= 7540 & df$SIC<= 7549)
            |(df$SIC>= 7600 & df$SIC<= 7600)
            |(df$SIC>= 7620 & df$SIC<= 7620)
            |(df$SIC>= 7622 & df$SIC<= 7622)
            |(df$SIC>= 7623 & df$SIC<= 7623)
            |(df$SIC>= 7629 & df$SIC<= 7629)
            |(df$SIC>= 7630 & df$SIC<= 7631)
            |(df$SIC>= 7640 & df$SIC<= 7641)
            |(df$SIC>= 7690 & df$SIC<= 7699)
            |(df$SIC>= 8100 & df$SIC<= 8199)
            |(df$SIC>= 8200 & df$SIC<= 8299)
            |(df$SIC>= 8300 & df$SIC<= 8399)
            |(df$SIC>= 8400 & df$SIC<= 8499)
            |(df$SIC>= 8600 & df$SIC<= 8699)
            |(df$SIC>= 8800 & df$SIC<= 8899)
            |(df$SIC>= 7510 & df$SIC<= 7515)] = 33
df$industry.name[df$industry==33] = "PerSv"

df$industry[(df$SIC>= 2750 & df$SIC<= 2759)
            |(df$SIC>= 3993 & df$SIC<= 3993)
            |(df$SIC>= 7218 & df$SIC<= 7218)
            |(df$SIC>= 7300 & df$SIC<= 7300)
            |(df$SIC>= 7310 & df$SIC<= 7319)
            |(df$SIC>= 7320 & df$SIC<= 7329)
            |(df$SIC>= 7330 & df$SIC<= 7339)
            |(df$SIC>= 7340 & df$SIC<= 7342)
            |(df$SIC>= 7349 & df$SIC<= 7349)
            |(df$SIC>= 7350 & df$SIC<= 7351)
            |(df$SIC>= 7352 & df$SIC<= 7352)
            |(df$SIC>= 7353 & df$SIC<= 7353)
            |(df$SIC>= 7359 & df$SIC<= 7359)
            |(df$SIC>= 7360 & df$SIC<= 7369)
            |(df$SIC>= 7370 & df$SIC<= 7372)
            |(df$SIC>= 7374 & df$SIC<= 7374)
            |(df$SIC>= 7375 & df$SIC<= 7375)
            |(df$SIC>= 7376 & df$SIC<= 7376)
            |(df$SIC>= 7377 & df$SIC<= 7377)
            |(df$SIC>= 7378 & df$SIC<= 7378)
            |(df$SIC>= 7379 & df$SIC<= 7379)
            |(df$SIC>= 7380 & df$SIC<= 7380)
            |(df$SIC>= 7381 & df$SIC<= 7382)
            |(df$SIC>= 7383 & df$SIC<= 7383)
            |(df$SIC>= 7384 & df$SIC<= 7384)
            |(df$SIC>= 7385 & df$SIC<= 7385)
            |(df$SIC>= 7389 & df$SIC<= 7390)
            |(df$SIC>= 7391 & df$SIC<= 7391)
            |(df$SIC>= 7392 & df$SIC<= 7392)
            |(df$SIC>= 7393 & df$SIC<= 7393)
            |(df$SIC>= 7394 & df$SIC<= 7394)
            |(df$SIC>= 7396 & df$SIC<= 7396)
            |(df$SIC>= 7397 & df$SIC<= 7397)
            |(df$SIC>= 7399 & df$SIC<= 7399)
            |(df$SIC>= 7519 & df$SIC<= 7519)
            |(df$SIC>= 8700 & df$SIC<= 8700)
            |(df$SIC>= 8710 & df$SIC<= 8713)
            |(df$SIC>= 8720 & df$SIC<= 8721)
            |(df$SIC>= 8730 & df$SIC<= 8734)
            |(df$SIC>= 8740 & df$SIC<= 8748)
            |(df$SIC>= 8900 & df$SIC<= 8910)
            |(df$SIC>= 8911 & df$SIC<= 8911)
            |(df$SIC>= 8920 & df$SIC<= 8999)
            |(df$SIC>= 4220 & df$SIC<= 4229)] = 34
df$industry.name[df$industry==34] = "BusSv"

df$industry[(df$SIC>= 3570 & df$SIC<= 3579)
            |(df$SIC>= 3680 & df$SIC<= 3680)
            |(df$SIC>= 3681 & df$SIC<= 3681)
            |(df$SIC>= 3682 & df$SIC<= 3682)
            |(df$SIC>= 3683 & df$SIC<= 3683)
            |(df$SIC>= 3684 & df$SIC<= 3684)
            |(df$SIC>= 3685 & df$SIC<= 3685)
            |(df$SIC>= 3686 & df$SIC<= 3686)
            |(df$SIC>= 3687 & df$SIC<= 3687)
            |(df$SIC>= 3688 & df$SIC<= 3688)
            |(df$SIC>= 3689 & df$SIC<= 3689)
            |(df$SIC>= 3695 & df$SIC<= 3695)
            |(df$SIC>= 7373 & df$SIC<= 7373)] = 35
df$industry.name[df$industry==35] = "Comps"

df$industry[(df$SIC>= 3622 & df$SIC<= 3622)
            |(df$SIC>= 3661 & df$SIC<= 3661)
            |(df$SIC>= 3662 & df$SIC<= 3662)
            |(df$SIC>= 3663 & df$SIC<= 3663)
            |(df$SIC>= 3664 & df$SIC<= 3664)
            |(df$SIC>= 3665 & df$SIC<= 3665)
            |(df$SIC>= 3666 & df$SIC<= 3666)
            |(df$SIC>= 3669 & df$SIC<= 3669)
            |(df$SIC>= 3670 & df$SIC<= 3679)
            |(df$SIC>= 3810 & df$SIC<= 3810)
            |(df$SIC>= 3812 & df$SIC<= 3812)] = 36
df$industry.name[df$industry==36] = 'Chips'

df$industry[(df$SIC>= 3811 & df$SIC<= 3811)
            |(df$SIC>= 3820 & df$SIC<= 3820)
            |(df$SIC>= 3821 & df$SIC<= 3821)
            |(df$SIC>= 3822 & df$SIC<= 3822)
            |(df$SIC>= 3823 & df$SIC<= 3823)
            |(df$SIC>= 3824 & df$SIC<= 3824)
            |(df$SIC>= 3825 & df$SIC<= 3825)
            |(df$SIC>= 3826 & df$SIC<= 3826)
            |(df$SIC>= 3827 & df$SIC<= 3827)
            |(df$SIC>= 3829 & df$SIC<= 3829)
            |(df$SIC>= 3830 & df$SIC<= 3839)] = 37
df$industry.name[df$industry==37] = "LabEq"

df$industry[(df$SIC>= 2520 & df$SIC<= 2549)
            |(df$SIC>= 2600 & df$SIC<= 2639)
            |(df$SIC>= 2670 & df$SIC<= 2699)
            |(df$SIC>= 2760 & df$SIC<= 2761)
            |(df$SIC>= 3950 & df$SIC<= 3955)] = 38
df$industry.name[df$industry==38] = "Paper"

df$industry[(df$SIC>= 2440 & df$SIC<= 2449)
            |(df$SIC>= 2640 & df$SIC<= 2659)
            |(df$SIC>= 3220 & df$SIC<= 3221)
            |(df$SIC>= 3410 & df$SIC<= 3412)] = 39
df$industry.name[df$industry==39] = "Boxes"

df$industry[(df$SIC>= 4000 & df$SIC<= 4013)
            |(df$SIC>= 4040 & df$SIC<= 4049)
            |(df$SIC>= 4100 & df$SIC<= 4100)
            |(df$SIC>= 4110 & df$SIC<= 4119)
            |(df$SIC>= 4120 & df$SIC<= 4121)
            |(df$SIC>= 4130 & df$SIC<= 4131)
            |(df$SIC>= 4140 & df$SIC<= 4142)
            |(df$SIC>= 4150 & df$SIC<= 4151)
            |(df$SIC>= 4170 & df$SIC<= 4173)
            |(df$SIC>= 4190 & df$SIC<= 4199)
            |(df$SIC>= 4200 & df$SIC<= 4200)
            |(df$SIC>= 4210 & df$SIC<= 4219)
            |(df$SIC>= 4230 & df$SIC<= 4231)
            |(df$SIC>= 4240 & df$SIC<= 4249)
            |(df$SIC>= 4400 & df$SIC<= 4499)
            |(df$SIC>= 4500 & df$SIC<= 4599)
            |(df$SIC>= 4600 & df$SIC<= 4699)
            |(df$SIC>= 4700 & df$SIC<= 4700)
            |(df$SIC>= 4710 & df$SIC<= 4712)
            |(df$SIC>= 4720 & df$SIC<= 4729)
            |(df$SIC>= 4730 & df$SIC<= 4739)
            |(df$SIC>= 4740 & df$SIC<= 4749)
            |(df$SIC>= 4780 & df$SIC<= 4780)
            |(df$SIC>= 4782 & df$SIC<= 4782)
            |(df$SIC>= 4783 & df$SIC<= 4783)
            |(df$SIC>= 4784 & df$SIC<= 4784)
            |(df$SIC>= 4785 & df$SIC<= 4785)
            |(df$SIC>= 4789 & df$SIC<= 4789)] = 40
df$industry.name[df$industry==40] = "Trams"

df$industry[(df$SIC>= 5000 & df$SIC<= 5000)
            |(df$SIC>= 5010 & df$SIC<= 5015)
            |(df$SIC>= 5020 & df$SIC<= 5023)
            |(df$SIC>= 5030 & df$SIC<= 5039)
            |(df$SIC>= 5040 & df$SIC<= 5042)
            |(df$SIC>= 5043 & df$SIC<= 5043)
            |(df$SIC>= 5044 & df$SIC<= 5044)
            |(df$SIC>= 5045 & df$SIC<= 5045)
            |(df$SIC>= 5046 & df$SIC<= 5046)
            |(df$SIC>= 5047 & df$SIC<= 5047)
            |(df$SIC>= 5048 & df$SIC<= 5048)
            |(df$SIC>= 5049 & df$SIC<= 5049)
            |(df$SIC>= 5050 & df$SIC<= 5059)
            |(df$SIC>= 5060 & df$SIC<= 5060)
            |(df$SIC>= 5063 & df$SIC<= 5063)
            |(df$SIC>= 5064 & df$SIC<= 5064)
            |(df$SIC>= 5065 & df$SIC<= 5065)
            |(df$SIC>= 5070 & df$SIC<= 5078)
            |(df$SIC>= 5080 & df$SIC<= 5080)
            |(df$SIC>= 5081 & df$SIC<= 5081)
            |(df$SIC>= 5082 & df$SIC<= 5082)
            |(df$SIC>= 5083 & df$SIC<= 5083)
            |(df$SIC>= 5084 & df$SIC<= 5084)
            |(df$SIC>= 5085 & df$SIC<= 5085)
            |(df$SIC>= 5086 & df$SIC<= 5087)
            |(df$SIC>= 5088 & df$SIC<= 5088)
            |(df$SIC>= 5090 & df$SIC<= 5090)
            |(df$SIC>= 5091 & df$SIC<= 5092)
            |(df$SIC>= 5093 & df$SIC<= 5093)
            |(df$SIC>= 5094 & df$SIC<= 5094)
            |(df$SIC>= 5099 & df$SIC<= 5099)
            |(df$SIC>= 5100 & df$SIC<= 5100)
            |(df$SIC>= 5110 & df$SIC<= 5113)
            |(df$SIC>= 5120 & df$SIC<= 5122)
            |(df$SIC>= 5130 & df$SIC<= 5139)
            |(df$SIC>= 5140 & df$SIC<= 5149)
            |(df$SIC>= 5150 & df$SIC<= 5159)
            |(df$SIC>= 5160 & df$SIC<= 5169)
            |(df$SIC>= 5170 & df$SIC<= 5172)
            |(df$SIC>= 5180 & df$SIC<= 5182)
            |(df$SIC>= 5190 & df$SIC<= 5199)] = 41
df$industry.name[df$industry==41] = "Whlsl"

df$industry[(df$SIC>= 5200 & df$SIC<= 5200)
            |(df$SIC>= 5210 & df$SIC<= 5219)
            |(df$SIC>= 5220 & df$SIC<= 5229)
            |(df$SIC>= 5230 & df$SIC<= 5231)
            |(df$SIC>= 5250 & df$SIC<= 5251)
            |(df$SIC>= 5260 & df$SIC<= 5261)
            |(df$SIC>= 5270 & df$SIC<= 5271)
            |(df$SIC>= 5300 & df$SIC<= 5300)
            |(df$SIC>= 5310 & df$SIC<= 5311)
            |(df$SIC>= 5320 & df$SIC<= 5320)
            |(df$SIC>= 5330 & df$SIC<= 5331)
            |(df$SIC>= 5334 & df$SIC<= 5334)
            |(df$SIC>= 5340 & df$SIC<= 5349)
            |(df$SIC>= 5390 & df$SIC<= 5399)
            |(df$SIC>= 5400 & df$SIC<= 5400)
            |(df$SIC>= 5410 & df$SIC<= 5411)
            |(df$SIC>= 5412 & df$SIC<= 5412)
            |(df$SIC>= 5420 & df$SIC<= 5429)
            |(df$SIC>= 5430 & df$SIC<= 5439)
            |(df$SIC>= 5440 & df$SIC<= 5449)
            |(df$SIC>= 5450 & df$SIC<= 5459)
            |(df$SIC>= 5460 & df$SIC<= 5469)
            |(df$SIC>= 5490 & df$SIC<= 5499)
            |(df$SIC>= 5500 & df$SIC<= 5500)
            |(df$SIC>= 5510 & df$SIC<= 5529)
            |(df$SIC>= 5530 & df$SIC<= 5539)
            |(df$SIC>= 5540 & df$SIC<= 5549)
            |(df$SIC>= 5550 & df$SIC<= 5559)
            |(df$SIC>= 5560 & df$SIC<= 5569)
            |(df$SIC>= 5570 & df$SIC<= 5579)
            |(df$SIC>= 5590 & df$SIC<= 5599)
            |(df$SIC>= 5600 & df$SIC<= 5699)
            |(df$SIC>= 5700 & df$SIC<= 5700)
            |(df$SIC>= 5710 & df$SIC<= 5719)
            |(df$SIC>= 5720 & df$SIC<= 5722)
            |(df$SIC>= 5730 & df$SIC<= 5733)
            |(df$SIC>= 5734 & df$SIC<= 5734)
            |(df$SIC>= 5735 & df$SIC<= 5735)
            |(df$SIC>= 5736 & df$SIC<= 5736)
            |(df$SIC>= 5750 & df$SIC<= 5799)
            |(df$SIC>= 5900 & df$SIC<= 5900)
            |(df$SIC>= 5910 & df$SIC<= 5912)
            |(df$SIC>= 5920 & df$SIC<= 5929)
            |(df$SIC>= 5930 & df$SIC<= 5932)
            |(df$SIC>= 5940 & df$SIC<= 5940)
            |(df$SIC>= 5941 & df$SIC<= 5941)
            |(df$SIC>= 5942 & df$SIC<= 5942)
            |(df$SIC>= 5943 & df$SIC<= 5943)
            |(df$SIC>= 5944 & df$SIC<= 5944)
            |(df$SIC>= 5945 & df$SIC<= 5945)
            |(df$SIC>= 5946 & df$SIC<= 5946)
            |(df$SIC>= 5947 & df$SIC<= 5947)
            |(df$SIC>= 5948 & df$SIC<= 5948)
            |(df$SIC>= 5949 & df$SIC<= 5949)
            |(df$SIC>= 5950 & df$SIC<= 5959)
            |(df$SIC>= 5960 & df$SIC<= 5969)
            |(df$SIC>= 5970 & df$SIC<= 5979)
            |(df$SIC>= 5980 & df$SIC<= 5989)
            |(df$SIC>= 5990 & df$SIC<= 5990)
            |(df$SIC>= 5992 & df$SIC<= 5992)
            |(df$SIC>= 5993 & df$SIC<= 5993)
            |(df$SIC>= 5994 & df$SIC<= 5994)
            |(df$SIC>= 5995 & df$SIC<= 5995)
            |(df$SIC>= 5999 & df$SIC<= 5999)] = 42
df$industry.name[df$industry==42] = "Rtail"

df$industry[(df$SIC>= 5800 & df$SIC<= 5819)
            |(df$SIC>= 5820 & df$SIC<= 5829)
            |(df$SIC>= 5890 & df$SIC<= 5899)
            |(df$SIC>= 7000 & df$SIC<= 7000)
            |(df$SIC>= 7010 & df$SIC<= 7019)
            |(df$SIC>= 7040 & df$SIC<= 7049)
            |(df$SIC>= 7213 & df$SIC<= 7213)] = 43
df$industry.name[df$industry==43] = "Meals"

df$industry[(df$SIC>= 6000 & df$SIC<= 6000)
            |(df$SIC>= 6010 & df$SIC<= 6019)
            |(df$SIC>= 6020 & df$SIC<= 6020)
            |(df$SIC>= 6021 & df$SIC<= 6021)
            |(df$SIC>= 6022 & df$SIC<= 6022)
            |(df$SIC>= 6023 & df$SIC<= 6024)
            |(df$SIC>= 6025 & df$SIC<= 6025)
            |(df$SIC>= 6026 & df$SIC<= 6026)
            |(df$SIC>= 6027 & df$SIC<= 6027)
            |(df$SIC>= 6028 & df$SIC<= 6029)
            |(df$SIC>= 6030 & df$SIC<= 6036)
            |(df$SIC>= 6040 & df$SIC<= 6059)
            |(df$SIC>= 6060 & df$SIC<= 6062)
            |(df$SIC>= 6080 & df$SIC<= 6082)
            |(df$SIC>= 6090 & df$SIC<= 6099)
            |(df$SIC>= 6100 & df$SIC<= 6100)
            |(df$SIC>= 6110 & df$SIC<= 6111)
            |(df$SIC>= 6112 & df$SIC<= 6113)
            |(df$SIC>= 6120 & df$SIC<= 6129)
            |(df$SIC>= 6130 & df$SIC<= 6139)
            |(df$SIC>= 6140 & df$SIC<= 6149)
            |(df$SIC>= 6150 & df$SIC<= 6159)
            |(df$SIC>= 6160 & df$SIC<= 6169)
            |(df$SIC>= 6170 & df$SIC<= 6179)
            |(df$SIC>= 6190 & df$SIC<= 6199)] = 44
df$industry.name[df$industry==44] = "Banks"

df$industry[(df$SIC>=  6300 & df$SIC<= 6300)
            |(df$SIC>= 6310 & df$SIC<= 6319)
            |(df$SIC>= 6320 & df$SIC<= 6329)
            |(df$SIC>= 6330 & df$SIC<= 6331)
            |(df$SIC>= 6350 & df$SIC<= 6351)
            |(df$SIC>= 6360 & df$SIC<= 6361)
            |(df$SIC>= 6370 & df$SIC<= 6379)
            |(df$SIC>= 6390 & df$SIC<= 6399)
            |(df$SIC>= 6400 & df$SIC<= 6411)] = 45
df$industry.name[df$industry==45] = "Insur"

df$industry[(df$SIC>= 6500 & df$SIC<= 6500)
            |(df$SIC>= 6510 & df$SIC<= 6510)
            |(df$SIC>= 6512 & df$SIC<= 6512)
            |(df$SIC>= 6513 & df$SIC<= 6513)
            |(df$SIC>= 6514 & df$SIC<= 6514)
            |(df$SIC>= 6515 & df$SIC<= 6515)
            |(df$SIC>= 6517 & df$SIC<= 6519)
            |(df$SIC>= 6520 & df$SIC<= 6529)
            |(df$SIC>= 6530 & df$SIC<= 6531)
            |(df$SIC>= 6532 & df$SIC<= 6532)
            |(df$SIC>= 6540 & df$SIC<= 6541)
            |(df$SIC>= 6550 & df$SIC<= 6553)
            |(df$SIC>= 6590 & df$SIC<= 6599)
            |(df$SIC>= 6610 & df$SIC<= 6611)] = 46
df$industry.name[df$industry==46] = "RlEst"

df$industry[(df$SIC>= 6200 & df$SIC<= 6299)
            |(df$SIC>= 6700 & df$SIC<= 6700)
            |(df$SIC>= 6710 & df$SIC<= 6719)
            |(df$SIC>= 6720 & df$SIC<= 6722)
            |(df$SIC>= 6723 & df$SIC<= 6723)
            |(df$SIC>= 6724 & df$SIC<= 6724)
            |(df$SIC>= 6725 & df$SIC<= 6725)
            |(df$SIC>= 6726 & df$SIC<= 6726)
            |(df$SIC>= 6730 & df$SIC<= 6733)
            |(df$SIC>= 6740 & df$SIC<= 6779)
            |(df$SIC>= 6790 & df$SIC<= 6791)
            |(df$SIC>= 6792 & df$SIC<= 6792)
            |(df$SIC>= 6793 & df$SIC<= 6793)
            |(df$SIC>= 6794 & df$SIC<= 6794)
            |(df$SIC>= 6795 & df$SIC<= 6795)
            |(df$SIC>= 6798 & df$SIC<= 6798)
            |(df$SIC>= 6799 & df$SIC<= 6799)] = 47
df$industry.name[df$industry==47] = "Fin"

df$industry[(df$SIC>= 4950 & df$SIC<= 4959)
            |(df$SIC>= 4960 & df$SIC<= 4961)
            |(df$SIC>= 4970 & df$SIC<= 4971)
            |(df$SIC>= 4990 & df$SIC<= 4991) 
            |(df$SIC>= 9990 & df$SIC<= 9999)] = 48
df$industry.name[df$industry==48] = "Other"
save(df , file = "df.RData")



########################################################
### STEP 4: generate results                       
# This step generates tables that replicates the results
# from CGS (2008) and extends their findings
########################################################

########################################################
###  Table 1: Asset Growth Deciles: Financial and Return Characteristics 
# This table show the financial and return characteristics
# for stocks grouped into deciles by ASSETG (asset growth)
########################################################
rm(data.ccm,data.crsp)
df.adj <- df %>%
  mutate(ASSETS = at) %>%
  # also use the same time frame and drop financial firms
  filter((year>=1968 & year<=2002) & (SIC < 6000 | SIC >=7000))
#assign decile groups by assetg and generate table 1
df.group <- df.adj %>%
  group_by(year) %>%
  summarize(assetg.p10= quantile(ASSETG,probs=0.1,na.rm=TRUE),
         assetg.p20= quantile(ASSETG,probs=0.2,na.rm=TRUE),
         assetg.p30= quantile(ASSETG,probs=0.3,na.rm=TRUE),
         assetg.p40= quantile(ASSETG,probs=0.4,na.rm=TRUE),
         assetg.p50= quantile(ASSETG,probs=0.5,na.rm=TRUE),
         assetg.p60= quantile(ASSETG,probs=0.6,na.rm=TRUE),
         assetg.p70= quantile(ASSETG,probs=0.7,na.rm=TRUE),
         assetg.p80= quantile(ASSETG,probs=0.8,na.rm=TRUE),
         assetg.p90= quantile(ASSETG,probs=0.9,na.rm=TRUE)) %>%
  ungroup 
df.adj <- df.adj %>%
  merge(.,df.group,by="year",all.x=TRUE) %>%
  mutate(group = ifelse(ASSETG<assetg.p10, 1, ifelse(ASSETG<assetg.p20, 2, 
                 ifelse(ASSETG<assetg.p30, 3, ifelse(ASSETG<assetg.p40, 4,
                 ifelse(ASSETG<assetg.p50, 5, ifelse(ASSETG<assetg.p60, 6,
                 ifelse(ASSETG<assetg.p70, 7, ifelse(ASSETG<assetg.p80, 8, 
                 ifelse(ASSETG<assetg.p90, 9, ifelse(is.na(ASSETG),NA,10)))))))))))
save(df.adj,file="df.adj.RData")
load("df.adj.RData")
df.table <- df.adj %>%
  group_by(year,group) %>%
  summarize(ASSETG.g = median(ASSETG,na.rm=TRUE),
         L2ASSETG.g = median(L2ASSETG,na.rm=TRUE),
         ASSETS.g = median(ASSETS,na.rm=TRUE),
         MV.g = median(MV,na.rm=TRUE),
         MV.ag = mean(MV,na.rm=TRUE),
         BM.g = median(BM,na.rm=TRUE),
         EP.g = median(EP,na.rm=TRUE),
         Leverage.g = median(Leverage,na.rm=TRUE),
         ROA.g = median(ROA,na.rm=TRUE),
         BHRET6.g = median(BHRET6,na.rm=TRUE),
         BHRET36.g = median(BHRET36,na.rm=TRUE),
         ACCRUALS.g = median(ACCRUALS,na.rm=TRUE),
         ISSUANCE.g = median(ISSUANCE,na.rm=TRUE)) %>%
  ungroup %>%
  filter(!is.na(group))
table1 <- df.table %>%
  group_by(group) %>%
  summarise_all(funs(mean)) %>%
  ungroup
# calculate spread(10-1)
table1 <- rbind(table1, table1[10, ] - table1[1, ])
# calculate t(spread)
table1[12,] <- NA
table1[12,"ASSETG.g"]=as.numeric(t.test(filter(df.table,group==10)$ASSETG.g,filter(df.table,group==1)$ASSETG.g,paired = TRUE)$statistic)
table1[12,"L2ASSETG.g"]=as.numeric(t.test(filter(df.table,group==10)$L2ASSETG.g,filter(df.table,group==1)$L2ASSETG.g,paired = TRUE)$statistic)
table1[12,"ASSETS.g"]=as.numeric(t.test(filter(df.table,group==10)$ASSETS.g,filter(df.table,group==1)$ASSETS.g,paired = TRUE)$statistic)
table1[12,"MV.g"]=as.numeric(t.test(filter(df.table,group==10)$MV.g,filter(df.table,group==1)$MV.g,paired = TRUE)$statistic)
table1[12,"MV.ag"]=as.numeric(t.test(filter(df.table,group==10)$MV.ag,filter(df.table,group==1)$MV.ag,paired = TRUE)$statistic)
table1[12,"BM.g"]=as.numeric(t.test(filter(df.table,group==10)$BM.g,filter(df.table,group==1)$BM.g,paired = TRUE)$statistic)
table1[12,"EP.g"]=as.numeric(t.test(filter(df.table,group==10)$EP.g,filter(df.table,group==1)$EP.g,paired = TRUE)$statistic)
table1[12,"Leverage.g"]=as.numeric(t.test(filter(df.table,group==10)$Leverage.g,filter(df.table,group==1)$Leverage.g,paired = TRUE)$statistic)
table1[12,"ROA.g"]=as.numeric(t.test(filter(df.table,group==10)$ROA.g,filter(df.table,group==1)$ROA.g,paired = TRUE)$statistic)
table1[12,"BHRET6.g"]=as.numeric(t.test(filter(df.table,group==10)$BHRET6.g,filter(df.table,group==1)$BHRET6.g,paired = TRUE)$statistic)
table1[12,"BHRET36.g"]=as.numeric(t.test(filter(df.table,group==10)$BHRET36.g,filter(df.table,group==1)$BHRET36.g,paired = TRUE)$statistic)
table1[12,"ACCRUALS.g"]=as.numeric(t.test(filter(df.table,group==10)$ACCRUALS.g,filter(df.table,group==1)$ACCRUALS.g,paired = TRUE)$statistic)
table1[12,"ISSUANCE.g"]=as.numeric(t.test(filter(df.table,group==10)$ISSUANCE.g,filter(df.table,group==1)$ISSUANCE.g,paired = TRUE)$statistic)
write.csv(table1,file="table1.csv")


########################################################
###  Table 2: Asset Growth Decile Portfolio Returns and Characteristics in Event Time
# This table shows the average asset growth rates and 
# raw portfolio returns for 10 years around the portfolio formation
# as well as the equal- and value-weighted portfolio 
# Fama-French alphas in the first year after portfolio formation
# that are grouped by firm size into small, median, and large groups
# and grouped by time period into 1968-1980, 1981-1990, 1991-2003, and 2004-2018
########################################################
#### panel A: average annual asset growth rates ####
df.adj2 <- df.adj %>%
  select(year,permno,ASSETG,group) %>%
  arrange(year,permno) %>%
  group_by(permno) %>%
  mutate(ASSETG1 = lead(ASSETG,1),
         ASSETG2 = lead(ASSETG,2),
         ASSETG3 = lead(ASSETG,3),
         ASSETG4 = lead(ASSETG,4),
         ASSETG5 = lead(ASSETG,5),
         ASSETG.n2 = lag(ASSETG,1),
         ASSETG.n3 = lag(ASSETG,2),
         ASSETG.n4 = lag(ASSETG,3),
         ASSETG.n5 = lag(ASSETG,4),) %>%
  ungroup
# calculate annual group level median
df.adj2 <- df.adj2 %>%
  group_by(group,year) %>%
  summarise_at(vars(ASSETG:ASSETG.n5), median, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
# calculate annual mean
df.table <- df.adj2 %>%
  group_by(group) %>%
  summarise_at(vars(ASSETG:ASSETG.n5), mean, na.rm = TRUE) %>%
  ungroup
# calculate spread(10-1)
df.table <- rbind(df.table, df.table[10, ] - df.table[1, ])
# calculate t(spread)
df.table[12,] <- NA
df.table[12,"ASSETG"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG,filter(df.adj2,group==1)$ASSETG,paired = TRUE)$statistic)
df.table[12,"ASSETG1"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG1,filter(df.adj2,group==1)$ASSETG1,paired = TRUE)$statistic)
df.table[12,"ASSETG2"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG2,filter(df.adj2,group==1)$ASSETG2,paired = TRUE)$statistic)
df.table[12,"ASSETG3"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG3,filter(df.adj2,group==1)$ASSETG3,paired = TRUE)$statistic)
df.table[12,"ASSETG4"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG4,filter(df.adj2,group==1)$ASSETG4,paired = TRUE)$statistic)
df.table[12,"ASSETG5"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG5,filter(df.adj2,group==1)$ASSETG5,paired = TRUE)$statistic)
df.table[12,"ASSETG.n2"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG.n2,filter(df.adj2,group==1)$ASSETG.n2,paired = TRUE)$statistic)
df.table[12,"ASSETG.n3"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG.n3,filter(df.adj2,group==1)$ASSETG.n3,paired = TRUE)$statistic)
df.table[12,"ASSETG.n4"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG.n4,filter(df.adj2,group==1)$ASSETG.n4,paired = TRUE)$statistic)
df.table[12,"ASSETG.n5"]=as.numeric(t.test(filter(df.adj2,group==10)$ASSETG.n5,filter(df.adj2,group==1)$ASSETG.n5,paired = TRUE)$statistic)
write.csv(df.table,file="table2 - panel A.csv")

#### panel B: 1. equal weighted portfolio return####
load("df.adj.RData")
df.adj2 <- df.adj %>%
  select(year,permno,ASSETG,group, ret.1:ret.12, MV) %>%
  arrange(year,permno) %>%
  group_by(permno) %>%
  mutate_at(vars(contains('ret')),
            .funs = list(f2 = ~lead(., 1), f3 = ~lead(.,2), f4 = ~lead(.,3), f5 = ~lead(.,4), 
                        l1 = ~lag(.,1),l2 = ~lag(.,2), l3 = ~lag(.,3), l4 = ~lag(.,4), l5 = ~lag(.,5))) %>%
  ungroup
# calculate ew return for 10 portfolios
df.adj2.table <- df.adj2 %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12_l5), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
# calculate average monthly return
table2 = data.frame(row.names = (seq(10)))
for (i in 1:10){
  temp <- df.adj2.table %>%
    filter(group==i) %>%
    select(ret.1:ret.12)
  table2[i,"f1"] <- mean(as.matrix(temp),na.rm=TRUE)
  for (j in c("f2","f3","f4","f5","l1","l2","l3","l4","l5")){
    temp <- df.adj2.table %>%
      filter(group==i) %>%
      select(ends_with(j))
    table2[i,j] <- mean(as.matrix(temp),na.rm=TRUE)
  }
}
table2 <- rbind(table2, table2[10, ] - table2[1, ])
table2[12,] <- NA
# calculate t(spread)
table2[12,"f1"] = as.numeric(t.test(as.matrix(dplyr::filter(df.adj2.table,group==10)[,c("ret.1","ret.2","ret.3","ret.4","ret.5","ret.6","ret.7","ret.8","ret.9","ret.10","ret.11","ret.12")]),
                                    as.matrix(dplyr::filter(df.adj2.table,group==1)[,c("ret.1","ret.2","ret.3","ret.4","ret.5","ret.6","ret.7","ret.8","ret.9","ret.10","ret.11","ret.12")]),paired = TRUE)$statistic)
for (i in c("f2","f3","f4","f5","l1","l2","l3","l4","l5")){
  table2[12,i] = as.numeric(t.test(as.matrix(dplyr::select(dplyr::filter(df.adj2.table,group==10),ends_with(i))),
                                      as.matrix(dplyr::select(dplyr::filter(df.adj2.table,group==1),ends_with(i))),paired = TRUE)$statistic)
}
write.csv(table2,file="table2 - panel B ew.csv")

# calculate (1,5) cumulative reuturn
table2 = data.frame(row.names = (seq(12)))
for (i in 1:10){
  temp <- df.adj2.table %>%
    filter(group==i) %>%
    select(starts_with("ret")) %>%
    select(-contains("l"))
  cum.ret = as.matrix(rep(1,35))
  for(j in colnames(temp)){
    cum.ret <- cum.ret * (1+as.matrix(temp[,j]))
  }
  table2[i,"(1,5)"] = mean(cum.ret,na.rm=TRUE)-1
}
table2[11,"(1,5)"] = table2[10,"(1,5)"] - table2[1,"(1,5)"]
# calculate t-stat
temp1 <- df.adj2.table %>%
  filter(group==1) %>%
  select(starts_with("ret")) %>%
  select(-contains("l"))
cum.ret1 = as.matrix(rep(1,35))
for(j in colnames(temp1)){
  cum.ret1 <- cum.ret1 * (1+as.matrix(temp1[,j]))
}
cum.ret1 <- cum.ret1 - as.matrix(rep(1,35))
temp10 <- df.adj2.table %>%
  filter(group==10) %>%
  select(starts_with("ret")) %>%
  select(-contains("l"))
cum.ret10 = as.matrix(rep(1,35))
for(j in colnames(temp10)){
  cum.ret10 <- cum.ret10 * (1+as.matrix(temp10[,j]))
}
cum.ret10 <- cum.ret10 - as.matrix(rep(1,35))
table2[12,"(1,5)"] = as.numeric(t.test(cum.ret10,cum.ret1, paired = TRUE)$statistic)

# calculate (-5,-1) cumulative reuturn
for (i in 1:10){
  temp <- df.adj2.table %>%
    filter(group==i) %>%
    select(contains("l"))
  cum.ret = as.matrix(rep(1,35))
  for(j in colnames(temp)){
    cum.ret <- cum.ret * (1+as.matrix(temp[,j]))
  }
  table2[i,"(-5,-1)"] = mean(cum.ret,na.rm=TRUE)-1
}
table2[11,"(-5,-1)"] = table2[10,"(-5,-1)"] - table2[1,"(-5,-1)"]
# calculate t-stat
temp1 <- df.adj2.table %>%
  filter(group==1) %>%
  select(contains("l"))
cum.ret1 = as.matrix(rep(1,35))
for(j in colnames(temp1)){
  cum.ret1 <- cum.ret1 * (1+as.matrix(temp1[,j]))
}
cum.ret1 <- cum.ret1 - as.matrix(rep(1,35))
temp10 <- df.adj2.table %>%
  filter(group==10) %>%
  select(contains("l"))
cum.ret10 = as.matrix(rep(1,35))
for(j in colnames(temp10)){
  cum.ret10 <- cum.ret10 * (1+as.matrix(temp10[,j]))
}
cum.ret10 <- cum.ret10 - as.matrix(rep(1,35))
table2[12,"(-5,-1)"] = as.numeric(t.test(cum.ret10,cum.ret1, paired = TRUE)$statistic)
write.csv(table2,file="table2 - panel B ew 5yr.csv")

#### panel B: 2. value weighted portfolio return####
# calculate vw return for 10 portfolios
df.adj2.table <- df.adj2 %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))

# calculate average monthly return
table2 = data.frame(row.names = (seq(10)))
for (i in 1:10){
  temp <- df.adj2.table %>%
    filter(group==i) %>%
    select(ret.1:ret.12)
  table2[i,"f1"] <- mean(as.matrix(temp),na.rm=TRUE)
  for (j in c("f2","f3","f4","f5","l1","l2","l3","l4","l5")){
    temp <- df.adj2.table %>%
      filter(group==i) %>%
      select(ends_with(j))
    table2[i,j] <- mean(as.matrix(temp),na.rm=TRUE)
  }
}
table2 <- rbind(table2, table2[10, ] - table2[1, ])
table2[12,] <- NA
# calculate t(spread)
table2[12,"f1"] = as.numeric(t.test(as.matrix(dplyr::filter(df.adj2.table,group==10)[,c("ret.1","ret.2","ret.3","ret.4","ret.5","ret.6","ret.7","ret.8","ret.9","ret.10","ret.11","ret.12")]),
                                    as.matrix(dplyr::filter(df.adj2.table,group==1)[,c("ret.1","ret.2","ret.3","ret.4","ret.5","ret.6","ret.7","ret.8","ret.9","ret.10","ret.11","ret.12")]),paired = TRUE)$statistic)
for (i in c("f2","f3","f4","f5","l1","l2","l3","l4","l5")){
  table2[12,i] = as.numeric(t.test(as.matrix(dplyr::select(dplyr::filter(df.adj2.table,group==10),ends_with(i))),
                                   as.matrix(dplyr::select(dplyr::filter(df.adj2.table,group==1),ends_with(i))),paired = TRUE)$statistic)
}
write.csv(table2,file="table2 - panel B vw.csv")

# calculate (1,5) cumulative reuturn
table2 = data.frame(row.names = (seq(12)))
for (i in 1:10){
  temp <- df.adj2.table %>%
    filter(group==i) %>%
    select(starts_with("ret")) %>%
    select(-contains("l"))
  cum.ret = as.matrix(rep(1,35))
  for(j in colnames(temp)){
    cum.ret <- cum.ret * (1+as.matrix(temp[,j]))
  }
  table2[i,"(1,5)"] = mean(cum.ret,na.rm=TRUE)-1
}
table2[11,"(1,5)"] = table2[10,"(1,5)"] - table2[1,"(1,5)"]
# calculate t-stat
temp1 <- df.adj2.table %>%
  filter(group==1) %>%
  select(starts_with("ret")) %>%
  select(-contains("l"))
cum.ret1 = as.matrix(rep(1,35))
for(j in colnames(temp1)){
  cum.ret1 <- cum.ret1 * (1+as.matrix(temp1[,j]))
}
cum.ret1 <- cum.ret1 - as.matrix(rep(1,35))
temp10 <- df.adj2.table %>%
  filter(group==10) %>%
  select(starts_with("ret")) %>%
  select(-contains("l"))
cum.ret10 = as.matrix(rep(1,35))
for(j in colnames(temp10)){
  cum.ret10 <- cum.ret10 * (1+as.matrix(temp10[,j]))
}
cum.ret10 <- cum.ret10 - as.matrix(rep(1,35))
table2[12,"(1,5)"] = as.numeric(t.test(cum.ret10,cum.ret1, paired = TRUE)$statistic)

# calculate (-5,-1) cumulative reuturn
for (i in 1:10){
  temp <- df.adj2.table %>%
    filter(group==i) %>%
    select(contains("l"))
  cum.ret = as.matrix(rep(1,35))
  for(j in colnames(temp)){
    cum.ret <- cum.ret * (1+as.matrix(temp[,j]))
  }
  table2[i,"(-5,-1)"] = mean(cum.ret,na.rm=TRUE)-1
}
table2[11,"(-5,-1)"] = table2[10,"(-5,-1)"] - table2[1,"(-5,-1)"]
# calculate t-stat
temp1 <- df.adj2.table %>%
  filter(group==1) %>%
  select(contains("l"))
cum.ret1 = as.matrix(rep(1,35))
for(j in colnames(temp1)){
  cum.ret1 <- cum.ret1 * (1+as.matrix(temp1[,j]))
}
cum.ret1 <- cum.ret1 - as.matrix(rep(1,35))
temp10 <- df.adj2.table %>%
  filter(group==10) %>%
  select(contains("l"))
cum.ret10 = as.matrix(rep(1,35))
for(j in colnames(temp10)){
  cum.ret10 <- cum.ret10 * (1+as.matrix(temp10[,j]))
}
cum.ret10 <- cum.ret10 - as.matrix(rep(1,35))
table2[12,"(-5,-1)"] = as.numeric(t.test(cum.ret10,cum.ret1, paired = TRUE)$statistic)
write.csv(table2,file="table2 - panel B vw 5yr.csv")


#### panel C: FF alphas grouped by size  ####
load("df.adj.RData")
# create breakpoints only use NYSE data
Bkpts.NYSE <- df.adj %>% # create size breakpoints based on NYSE stocks only
  filter(exchcd == 1) %>% # NYSE exchange
  group_by(year) %>%
  summarize(size.Large = quantile(MV, probs=.7, na.rm=TRUE),
            size.Small = quantile(MV, probs=.3, na.rm=TRUE))
df.adj <- df.adj %>%
  merge(.,Bkpts.NYSE,by="year",all.x=TRUE) %>%
  mutate(size.group = ifelse(MV<size.Small, 1, ifelse(MV<size.Large, 2, ifelse(is.na(MV),NA, 3))))
save(df.adj,file="df.adj.RData")
# import FF 3 factors 
FF3 = read.csv("F-F_Research_Data_Factors.csv")
FF3 = rename(FF3,yearmonth = X)
#generate the result table
table2c = data.frame(row.names=seq(12))
# generate portfolio returns for each group
df.adj.all <- df.adj %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.small <- df.adj %>%
  filter(size.group==1) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.med <- df.adj %>%
  filter(size.group==2) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.large <- df.adj %>%
  filter(size.group==3) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))

# generate FF 3 factor alphas
alpha <- function(df,type,table){
  df <- df %>%
    pivot_longer(
      cols = starts_with("ret."),
      names_to = "month",
      names_prefix = "ret.",
      values_to = "ret") %>%
    mutate(month = as.numeric(month),
           yearmonth = ifelse(month<=6,(year+1)*100+month,year*100+month)) #readjust the month values
  for (i in 1:10){
    temp <- df %>%
      filter(group==i) %>%
      merge(.,FF3,on="yearmonth", all.x= TRUE)  %>%
      mutate(ex.ret = ret-RF/100)
    reg = lm('ex.ret ~ 1 + Mkt.RF + SMB + HML', data=temp, na.action=na.exclude)
    table[i,type] = as.numeric(reg$coefficients[1])
  }
  return(table)
}
table2c = alpha(df.adj.all,"all",table2c)
table2c = alpha(df.adj.small,"small",table2c)
table2c = alpha(df.adj.med,"med",table2c)
table2c = alpha(df.adj.large,"large",table2c)
table2c[11,]=table2c[10,]-table2c[1,]
# calculate t-statistic
library(lmtest)
tstat <- function(df,type,table){
  df <- df %>%
    pivot_longer(
      cols = starts_with("ret."),
      names_to = "month",
      names_prefix = "ret.",
      values_to = "ret") %>%
    mutate(month = as.numeric(month),
         yearmonth = ifelse(month<=6,(year+1)*100+month,year*100+month)) #readjust the month values
  df.10 <- df %>%
    filter(group==10) %>%
    merge(.,FF3,on="yearmonth", all.x= TRUE)  %>%
    mutate(ex.ret.10 = ret-RF/100) %>%
    select(yearmonth,Mkt.RF,SMB,HML,RF,ex.ret.10)
  df.1 <- df %>%
    filter(group==1) %>%
    merge(.,df.10,on="yearmonth", all.x= TRUE)  %>%
    mutate(ex.ret.1 = ret-RF/100,
         ret.diff = ex.ret.10 - ex.ret.1)
  reg = lm("ret.diff ~ 1 + Mkt.RF + SMB + HML", data=df.1, na.action=na.exclude)
  table[12,type]=coeftest(reg, vcov = NeweyWest(reg))[1,3]
  return(table)
}  
table2c<-tstat(df.adj.all,"all",table2c)  
table2c<-tstat(df.adj.small,"small",table2c)  
table2c<-tstat(df.adj.med,"med",table2c)  
table2c<-tstat(df.adj.large,"large",table2c)  
write.csv(table2c,file="table2 - panel c ew.csv")

# calculate value weighted results  
# generate weighted portfolio returns for each group
df.adj.all <- df.adj %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.small <- df.adj %>%
  filter(size.group==1) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.med <- df.adj %>%
  filter(size.group==2) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.large <- df.adj %>%
  filter(size.group==3) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
#generate the result table
table2c = data.frame(row.names=seq(12))
table2c = alpha(df.adj.all,"all",table2c)
table2c = alpha(df.adj.small,"small",table2c)
table2c = alpha(df.adj.med,"med",table2c)
table2c = alpha(df.adj.large,"large",table2c)
table2c[11,]=table2c[10,]-table2c[1,]
table2c<-tstat(df.adj.all,"all",table2c)  
table2c<-tstat(df.adj.small,"small",table2c)  
table2c<-tstat(df.adj.med,"med",table2c)  
table2c<-tstat(df.adj.large,"large",table2c)  
write.csv(table2c,file="table2 - panel c vw.csv")


#### panel D: FF alphas grouped by time ####
# in this part I want to add the up-to-date analysis of year 2004 to 2018
# I group results into four groups by time :  1968-1980, 1981-1990, 1991-2003, and 2004-2018
load("df.RData")
df.adj <- df %>%
  mutate(ASSETS = at) %>%
  # also use the same time frame and drop financial firms
  filter((year>=1968 & year<=2018) & (SIC < 6000 | SIC >=7000))
#assign decile groups by assetg and generate table 1
df.group <- df.adj %>%
  group_by(year) %>%
  summarize(assetg.p10= quantile(ASSETG,probs=0.1,na.rm=TRUE),
            assetg.p20= quantile(ASSETG,probs=0.2,na.rm=TRUE),
            assetg.p30= quantile(ASSETG,probs=0.3,na.rm=TRUE),
            assetg.p40= quantile(ASSETG,probs=0.4,na.rm=TRUE),
            assetg.p50= quantile(ASSETG,probs=0.5,na.rm=TRUE),
            assetg.p60= quantile(ASSETG,probs=0.6,na.rm=TRUE),
            assetg.p70= quantile(ASSETG,probs=0.7,na.rm=TRUE),
            assetg.p80= quantile(ASSETG,probs=0.8,na.rm=TRUE),
            assetg.p90= quantile(ASSETG,probs=0.9,na.rm=TRUE)) %>%
  ungroup 
df.adj <- df.adj %>%
  merge(.,df.group,by="year",all.x=TRUE) %>%
  mutate(group = ifelse(ASSETG<assetg.p10, 1, ifelse(ASSETG<assetg.p20, 2, 
                ifelse(ASSETG<assetg.p30, 3, ifelse(ASSETG<assetg.p40, 4,
                ifelse(ASSETG<assetg.p50, 5, ifelse(ASSETG<assetg.p60, 6,
                ifelse(ASSETG<assetg.p70, 7, ifelse(ASSETG<assetg.p80, 8, 
                ifelse(ASSETG<assetg.p90, 9, ifelse(is.na(ASSETG),NA,10))))))))))) 

df.adj.6880 <- df.adj %>%
  filter(year>=1968 & year<=1980) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.8190 <- df.adj %>%
  filter(year>=1981 & year<=1990) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.9103 <- df.adj %>%
  filter(year>=1991 & year<=2003) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.0418 <- df.adj %>%
  filter(year>=2004 & year<=2018) %>%
  group_by(group,year) %>%
  summarise_at(vars(ret.1:ret.12), mean, na.rm = TRUE) %>%
  ungroup %>%
  filter(!is.na(group))
table2d = data.frame(row.names=seq(12))
table2d = alpha(df.adj.6880,"6880",table2d)
table2d = alpha(df.adj.8190,"8190",table2d)
table2d = alpha(df.adj.9103,"9103",table2d)
table2d = alpha(df.adj.0418,"0418",table2d)
table2d[11,]=table2d[10,]-table2d[1,]
table2d<-tstat(df.adj.6880,"6880",table2d)  
table2d<-tstat(df.adj.8190,"8190",table2d)  
table2d<-tstat(df.adj.9103,"9103",table2d)  
table2d<-tstat(df.adj.0418,"0418",table2d)  
write.csv(table2d,file="table2 - panel d ew.csv")

# calcualte value weighted returns
df.adj.6880 <- df.adj %>%
  filter(year>=1968 & year<=1980) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.8190 <- df.adj %>%
  filter(year>=1981 & year<=1990) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.9103 <- df.adj %>%
  filter(year>=1991 & year<=2003) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
df.adj.0418 <- df.adj %>%
  filter(year>=2004 & year<=2018) %>%
  select(year,group,MV,ret.1:ret.12) %>%
  group_by(group,year) %>%
  summarise_each(funs(weighted.mean(., MV, na.rm = TRUE)),-MV) %>%
  ungroup %>%
  filter(!is.na(group))
table2d = data.frame(row.names=seq(12))
table2d = alpha(df.adj.6880,"6880",table2d)
table2d = alpha(df.adj.8190,"8190",table2d)
table2d = alpha(df.adj.9103,"9103",table2d)
table2d = alpha(df.adj.0418,"0418",table2d)
table2d[11,]=table2d[10,]-table2d[1,]
table2d<-tstat(df.adj.6880,"6880",table2d)  
table2d<-tstat(df.adj.8190,"8190",table2d)  
table2d<-tstat(df.adj.9103,"9103",table2d)  
table2d<-tstat(df.adj.0418,"0418",table2d)  
write.csv(table2d,file="table2 - panel d vw.csv")



########################################################
###  Table 3: Fama-MacBeth Regressions of Annual Stock Returns on Asset Growth and Other Variables
# This table shows that the asset growth anomoly is strong
# after controlling for lagged asset growth rate, book-to-market ratio,
# short- and long-term return, abnormal capital investment, and accruals
# I also group firms by size and show that the result is 
# strongest for small firms
########################################################


load("df.adj.RData")
require(plm)
require(lmtest)
library(sandwich)

FamaMacBeth = function(coef,data,row1,row2,table=table3,start=1968,end=2002){
  beta.ts = data.frame(row.names=seq((end-start+1)))
  f <- paste("ret.1yr ~ 1 +" ,paste(coef, collapse=" + "))
  for (i in start:end){
    reg <- try(lm(f,data=filter(data,data$year==i), na.action=na.exclude), silent=TRUE)
    if ('try-error' %in% class(reg)) next
    else reg <- reg
    beta.ts[(i-start+1),"constant"]= reg$coefficients[1]
    for (j in 1:length(coef)){
      beta.ts[(i-start+1),coef[j]]= reg$coefficients[j+1]
    }
  }
  table[row1,"constant"]= mean(beta.ts[,"constant"],na.rm=TRUE)
  reg = lm("constant~1",data=beta.ts, na.action=na.exclude)
  table[row2,"constant"]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
  for (i in 1:length(coef)){
    table[row1,coef[i]]= mean(beta.ts[,coef[i]],na.rm=TRUE)
    reg = lm(paste(coef[i]," ~ 1"),data=beta.ts, na.action=na.exclude)
    table[row2,coef[i]]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
  }
  return(table)
}
table3 = data.frame(row.names=seq(14))
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36"),data = df.adj,1,2)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","L2ASSETG"),data = df.adj,3,4)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","SALESG5Y"),data = df.adj,5,6)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","CI"),data = df.adj,7,8)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","NOA.A"),data = df.adj,9,10)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ACCRUALS"),data = df.adj,11,12)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ASSETG5Y"),data = df.adj,13,14)
write.csv(table3,"table3 - all.csv")
# group by size - small
table3 = data.frame(row.names=seq(14))
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36"),data = filter(df.adj,df.adj$size.group==1),1,2)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","L2ASSETG"),data = filter(df.adj,df.adj$size.group==1),3,4)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","SALESG5Y"),data = filter(df.adj,df.adj$size.group==1),5,6)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","NOA.A"),data = filter(df.adj,df.adj$size.group==1),7,8)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ACCRUALS"),data = filter(df.adj,df.adj$size.group==1),9,10)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ASSETG5Y"),data = filter(df.adj,df.adj$size.group==1),11,12)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","CI"),data = filter(df.adj,df.adj$size.group==1),13,14)
write.csv(table3,"table3 - small.csv")
#median
table3 = data.frame(row.names=seq(14))
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36"),data = filter(df.adj,df.adj$size.group==2),1,2)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","L2ASSETG"),data = filter(df.adj,df.adj$size.group==2),3,4)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","SALESG5Y"),data = filter(df.adj,df.adj$size.group==2),5,6)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","NOA.A"),data = filter(df.adj,df.adj$size.group==2),7,8)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ACCRUALS"),data = filter(df.adj,df.adj$size.group==2),9,10)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ASSETG5Y"),data = filter(df.adj,df.adj$size.group==2),11,12)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","CI"),data = filter(df.adj,df.adj$size.group==2),13,14)
write.csv(table3,"table3 - median.csv")
#large
table3 = data.frame(row.names=seq(14))
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36"),data = filter(df.adj,df.adj$size.group==3),1,2)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","L2ASSETG"),data = filter(df.adj,df.adj$size.group==3),3,4)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","SALESG5Y"),data = filter(df.adj,df.adj$size.group==3),5,6)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","NOA.A"),data = filter(df.adj,df.adj$size.group==3),7,8)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ACCRUALS"),data = filter(df.adj,df.adj$size.group==3),9,10)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","ASSETG5Y"),data = filter(df.adj,df.adj$size.group==3),11,12)
table3 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36","CI"),data = filter(df.adj,df.adj$size.group==3),13,14)
write.csv(table3,"table3 - large.csv")



########################################################
### Table 4: Fama-MacBeth Annual Stock Return Regressions: Asset and Financing Decompositions
# Annual stock returns are regressed on variables obtained from a balance sheet
# decomposition of asset growth into an investment aspect and a financing aspect
########################################################
table4 = data.frame(row.names=seq(20))
table4 <- FamaMacBeth(coef = c("Cashg"),data = df.adj,1,2,table=table4)
table4 <- FamaMacBeth(coef = c("CurAsstg"),data = df.adj,3,4,table=table4)
table4 <- FamaMacBeth(coef = c("PPEg"),data = df.adj,5,6,table=table4)
table4 <- FamaMacBeth(coef = c("Othersg"),data = df.adj,7,8,table=table4)
table4 <- FamaMacBeth(coef = c("Cashg","CurAsstg","PPEg","Othersg"),data = df.adj,9,10,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg"),data = df.adj,11,12,table=table4)
table4 <- FamaMacBeth(coef = c("Debtg"),data = df.adj,13,14,table=table4)
table4 <- FamaMacBeth(coef = c("StockFing"),data = df.adj,15,16,table=table4)
table4 <- FamaMacBeth(coef = c("REg"),data = df.adj,17,18,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg","Debtg","StockFing","REg"),data = df.adj,19,20,table=table4)
write.csv(table4,"table4 - all.csv")

# small firms
table4 = data.frame(row.names=seq(20))
table4 <- FamaMacBeth(coef = c("Cashg"),data = filter(df.adj,df.adj$size.group==1),1,2,table=table4)
table4 <- FamaMacBeth(coef = c("CurAsstg"),data = filter(df.adj,df.adj$size.group==1),3,4,table=table4)
table4 <- FamaMacBeth(coef = c("PPEg"),data = filter(df.adj,df.adj$size.group==1),5,6,table=table4)
table4 <- FamaMacBeth(coef = c("Othersg"),data = filter(df.adj,df.adj$size.group==1),7,8,table=table4)
table4 <- FamaMacBeth(coef = c("Cashg","CurAsstg","PPEg","Othersg"),data = filter(df.adj,df.adj$size.group==1),9,10,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg"),data = filter(df.adj,df.adj$size.group==1),11,12,table=table4)
table4 <- FamaMacBeth(coef = c("Debtg"),data = filter(df.adj,df.adj$size.group==1),13,14,table=table4)
table4 <- FamaMacBeth(coef = c("StockFing"),data = filter(df.adj,df.adj$size.group==1),15,16,table=table4)
table4 <- FamaMacBeth(coef = c("REg"),data = filter(df.adj,df.adj$size.group==1),17,18,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg","Debtg","StockFing","REg"),data = filter(df.adj,df.adj$size.group==1),19,20,table=table4)
write.csv(table4,"table4 - small.csv")

# median firms
table4 = data.frame(row.names=seq(20))
table4 <- FamaMacBeth(coef = c("Cashg"),data = filter(df.adj,df.adj$size.group==2),1,2,table=table4)
table4 <- FamaMacBeth(coef = c("CurAsstg"),data = filter(df.adj,df.adj$size.group==2),3,4,table=table4)
table4 <- FamaMacBeth(coef = c("PPEg"),data = filter(df.adj,df.adj$size.group==2),5,6,table=table4)
table4 <- FamaMacBeth(coef = c("Othersg"),data = filter(df.adj,df.adj$size.group==2),7,8,table=table4)
table4 <- FamaMacBeth(coef = c("Cashg","CurAsstg","PPEg","Othersg"),data = filter(df.adj,df.adj$size.group==2),9,10,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg"),data = filter(df.adj,df.adj$size.group==2),11,12,table=table4)
table4 <- FamaMacBeth(coef = c("Debtg"),data = filter(df.adj,df.adj$size.group==2),13,14,table=table4)
table4 <- FamaMacBeth(coef = c("StockFing"),data = filter(df.adj,df.adj$size.group==2),15,16,table=table4)
table4 <- FamaMacBeth(coef = c("REg"),data = filter(df.adj,df.adj$size.group==2),17,18,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg","Debtg","StockFing","REg"),data = filter(df.adj,df.adj$size.group==2),19,20,table=table4)
write.csv(table4,"table4 - median.csv")

# large firms
table4 = data.frame(row.names=seq(20))
table4 <- FamaMacBeth(coef = c("Cashg"),data = filter(df.adj,df.adj$size.group==3),1,2,table=table4)
table4 <- FamaMacBeth(coef = c("CurAsstg"),data = filter(df.adj,df.adj$size.group==3),3,4,table=table4)
table4 <- FamaMacBeth(coef = c("PPEg"),data = filter(df.adj,df.adj$size.group==3),5,6,table=table4)
table4 <- FamaMacBeth(coef = c("Othersg"),data = filter(df.adj,df.adj$size.group==3),7,8,table=table4)
table4 <- FamaMacBeth(coef = c("Cashg","CurAsstg","PPEg","Othersg"),data = filter(df.adj,df.adj$size.group==3),9,10,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg"),data = filter(df.adj,df.adj$size.group==3),11,12,table=table4)
table4 <- FamaMacBeth(coef = c("Debtg"),data = filter(df.adj,df.adj$size.group==3),13,14,table=table4)
table4 <- FamaMacBeth(coef = c("StockFing"),data = filter(df.adj,df.adj$size.group==3),15,16,table=table4)
table4 <- FamaMacBeth(coef = c("REg"),data = filter(df.adj,df.adj$size.group==3),17,18,table=table4)
table4 <- FamaMacBeth(coef = c("OpLiabg","Debtg","StockFing","REg"),data = filter(df.adj,df.adj$size.group==3),19,20,table=table4)
write.csv(table4,"table4 - large.csv")


########################################################
### Table 5: Industrs Portfolio Descriptive Statistics
# This table provides summary statistics for the 48 industries defined by Fama and French (1997)
########################################################
load("df.RData")
df <- df %>%
  filter(year>=1968 & year<=2018)
df[sapply(df, is.infinite)] <- NA
table5 = data.frame(row.names=seq(48))
for (i in 1:48){
  table5[i,"obs"]=length(df$industry[df$industry==i])
  table5[i,"total.mkt.value"] = sum(df$MV[df$industry==i],na.rm=TRUE)
  table5[i,"assetg.mean"] = mean(df$ASSETG[df$industry==i],na.rm=TRUE)
  table5[i,"assetg.median"] = median(df$ASSETG[df$industry==i],na.rm=TRUE)
  table5[i,"assetg.stdev"] = sd(df$ASSETG[df$industry==i],na.rm=TRUE)
}
table5[49,"obs"]=length(df$industry)
table5[49,"total.mkt.value"] = sum(df$MV,na.rm=TRUE)
table5[49,"assetg.mean"] = mean(df$ASSETG,na.rm=TRUE)
table5[49,"assetg.median"] = median(df$ASSETG,na.rm=TRUE)
table5[49,"assetg.stdev"] = sd(df$ASSETG,na.rm=TRUE)
write.csv(table5,file="table5 - summary stats.csv")


########################################################
###  Table 6: Asset Growth and Stock Returns: Industry Level
# This table reports the equal- and value-weighted measures of the asset growth effect by industry
########################################################
table6 = data.frame()
for (i in 1:48){
  temp <- filter(df,df$industry==i)
  
  beta.ts = data.frame(row.names=seq((51)))
  for (t in 1968:2018){
    reg <- try(lm("ret.1yr~1+ASSETG",data=filter(temp,temp$year==t), na.action=na.exclude), silent=TRUE)
    if ('try-error' %in% class(reg)) next
    else reg <- reg
    beta.ts[(t-1967),"slope"]= reg$coefficients[2]
  }
  table6[i,"ew.slope"]= mean(beta.ts[,"slope"],na.rm=TRUE)
  reg = lm("slope~1",data=beta.ts, na.action=na.exclude)
  table6[i,"ew.slope.tstat"]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
 
  beta.ts = data.frame(row.names=seq((51)))
  for (t in 1968:2018){
    temp.t = filter(temp,temp$year==t)
    reg <- try(lm("ret.1yr~1+ASSETG",data=temp.t, na.action=na.exclude, weights = temp.t$MV), silent=TRUE)
    if ('try-error' %in% class(reg)) next
    else reg <- reg
    beta.ts[(t-1967),"slope"]= reg$coefficients[2]
  }
  table6[i,"vw.slope"]= mean(beta.ts[,"slope"],na.rm=TRUE)
  reg = lm("slope~1",data=beta.ts, na.action=na.exclude)
  table6[i,"vw.slope.tstat"]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
  
  if (length(temp$industry)/51 <50){  # if firms in each industry-year is smaller than 50, group into terciles
    temp.group <- temp %>%
      group_by(year) %>%
      summarize(assetg.p30= quantile(ASSETG,probs=0.3,na.rm=TRUE),
                assetg.p70= quantile(ASSETG,probs=0.7,na.rm=TRUE)) %>%
      ungroup
    temp <- temp %>%
      merge(.,temp.group,by="year",all.x=TRUE) %>%
      mutate(group = ifelse(ASSETG<assetg.p30, 1, ifelse(ASSETG>assetg.p70, 2, NA)))  #bottom portfolio is 1, top portfolio is 2
  } else if (length(temp$industry)/51 >= 100){  # if firms in each industry-year is greater than 100, group into deciles
    temp.group <- temp %>%
      group_by(year) %>%
      summarize(assetg.p10= quantile(ASSETG,probs=0.1,na.rm=TRUE),
                assetg.p90= quantile(ASSETG,probs=0.9,na.rm=TRUE)) %>%
      ungroup
    temp <- temp %>%
      merge(.,temp.group,by="year",all.x=TRUE) %>%
      mutate(group = ifelse(ASSETG<assetg.p10, 1, ifelse(ASSETG>assetg.p90, 2, NA)))  #bottom portfolio is 1, top portfolio is 2
  } else {          # if firms in each industry-year is between 50 and 100, group into quintiles
    temp.group <- temp %>%
      group_by(year) %>%
      summarize(assetg.p20= quantile(ASSETG,probs=0.2,na.rm=TRUE),
                assetg.p80= quantile(ASSETG,probs=0.8,na.rm=TRUE)) %>%
      ungroup
    temp <- temp %>%
      merge(.,temp.group,by="year",all.x=TRUE) %>%
      mutate(group = ifelse(ASSETG<assetg.p20, 1, ifelse(ASSETG>assetg.p80, 2, NA)))  #bottom portfolio is 1, top portfolio is 2
  }
  
  temp.group <- temp %>%
    group_by(group,year) %>%
    summarise_at(vars(ASSETG,ret.1yr), mean, na.rm = TRUE) %>%
    ungroup %>%
    filter(!is.na(group))
  temp.vwgroup <- temp %>%
    group_by(group,year) %>%
    summarise_at(vars(ret.1yr), funs(weighted.mean(., MV, na.rm = TRUE))) %>%
    ungroup %>%
    filter(!is.na(group))
  
  diff = filter(temp.group,group==2)-filter(temp.group,group==1)
  diff.vw =  filter(temp.vwgroup,group==2)-filter(temp.vwgroup,group==1)
  table6[i,"ASSETG"]=mean(diff$ASSETG,na.rm=TRUE)
  reg = lm("ASSETG~1",data=diff,na.action = na.exclude)
  table6[i,"ASSETG.tstat"]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
  table6[i,"ew.spread"]=mean(diff$ret.1yr,na.rm=TRUE)
  reg = lm("ret.1yr~1",data=diff,na.action = na.exclude)
  table6[i,"ew.spread.tstat"]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
  table6[i,"vw.spread"]=mean(diff.vw$ret.1yr,na.rm=TRUE)
  reg = lm("ret.1yr~1",data=diff.vw,na.action = na.exclude)
  table6[i,"vw.spread.tstat"]= as.numeric(coeftest(reg,vcov=NeweyWest(reg))[3])
}
write.csv(table6,"table6.csv")


########################################################
###  Table 7: Fama-MacBeth Regressions of Industry-Level Annual Stock Returns
# This table reports the Fama-MacBeth regression results by industry
########################################################
load("df.RData")
df.adj <- df %>%
  filter(year>=1968 & year<=2018) %>%
  arrange(year)

table7 = data.frame(row.names=seq(96))
for (g in 1:48){
  table7 <- FamaMacBeth(coef = c("ASSETG","BM","MV","BHRET6","BHRET36"),data = filter(df.adj,df.adj$industry==g),(g*2-1),(g*2),
                        table=table7,start=as.numeric(filter(df.adj,df.adj$industry==g)$year[1]),end=2018)
}
write.csv(table7,"table7.csv")



###  Table 8: Cross-Industry Analysis: Industry Concentration and the Asset Growth Effect
# Panel A reports the Pearson correlations between industry concentration measures 
# Panel B and C report the results of the cross-industry regressions 
# which examine the relationship between industry concentration and the asset growth effect.
########################################################

# get the panel for ew and vw spread and slope
panel.all = data.frame()
for (i in 1:48){
  temp <- filter(df,df$industry==i)
  panel = data.frame(row.names=seq(51))
  panel$year = seq(1:51) + 1967
  for (t in 1968:2018){
    reg <- try(lm("ret.1yr~1+ASSETG",data=filter(temp,temp$year==t), na.action=na.exclude), silent=TRUE)
    if ('try-error' %in% class(reg)) next
    else reg <- reg
    panel[(t-1967),"ew.slope"]= reg$coefficients[2]
  }
  for (t in 1968:2018){
    temp.t = filter(temp,temp$year==t)
    reg <- try(lm("ret.1yr~1+ASSETG",data=temp.t, na.action=na.exclude, weights = temp.t$MV), silent=TRUE)
    if ('try-error' %in% class(reg)) next
    else reg <- reg
    panel[(t-1967),"vw.slope"]= reg$coefficients[2]
  }
  # if firms in each industry-year is smaller than 50, group into terciles
    if (length(temp$industry)/51 <50){  
    temp.group <- temp %>%
      group_by(year) %>%
      summarize(assetg.p30= quantile(ASSETG,probs=0.3,na.rm=TRUE),
                assetg.p70= quantile(ASSETG,probs=0.7,na.rm=TRUE)) %>%
      ungroup
    temp <- temp %>%
      merge(.,temp.group,by="year",all.x=TRUE) %>%
      #bottom portfolio is 1, top portfolio is 2
      mutate(group = ifelse(ASSETG<assetg.p30, 1, ifelse(ASSETG>assetg.p70, 2, NA)))  
    # if firms in each industry-year is greater than 100, group into deciles
  } else if (length(temp$industry)/51 >= 100){  
    temp.group <- temp %>%
      group_by(year) %>%
      summarize(assetg.p10= quantile(ASSETG,probs=0.1,na.rm=TRUE),
                assetg.p90= quantile(ASSETG,probs=0.9,na.rm=TRUE)) %>%
      ungroup
    temp <- temp %>%
      merge(.,temp.group,by="year",all.x=TRUE) %>%
      #bottom portfolio is 1, top portfolio is 2
      mutate(group = ifelse(ASSETG<assetg.p10, 1, ifelse(ASSETG>assetg.p90, 2, NA)))  
  } else {          # if firms in each industry-year is between 50 and 100, group into quintiles
    temp.group <- temp %>%
      group_by(year) %>%
      summarize(assetg.p20= quantile(ASSETG,probs=0.2,na.rm=TRUE),
                assetg.p80= quantile(ASSETG,probs=0.8,na.rm=TRUE)) %>%
      ungroup
    temp <- temp %>%
      merge(.,temp.group,by="year",all.x=TRUE) %>%
      #bottom portfolio is 1, top portfolio is 2
      mutate(group = ifelse(ASSETG<assetg.p20, 1, ifelse(ASSETG>assetg.p80, 2, NA)))  
  }
  
  temp.group <- temp %>%
    group_by(group,year) %>%
    summarise_at(vars(ASSETG,ret.1yr), mean, na.rm = TRUE) %>%
    ungroup %>%
    filter(!is.na(group)) %>%
    rename(ewret = ret.1yr)
  temp.vwgroup <- temp %>%
    group_by(group,year) %>%
    summarise_at(vars(ret.1yr), funs(weighted.mean(., MV, na.rm = TRUE))) %>%
    ungroup %>%
    filter(!is.na(group)) %>%
    rename(vwret = ret.1yr)
  
  diff = filter(temp.group,group==2)$ewret-filter(temp.group,group==1)$ewret
  diff = as.data.frame(diff) 
  diff = rename(diff,ewret=diff)
  diff$year = filter(temp.group,group==2)$year
  diff.vw =  filter(temp.vwgroup,group==2)$vwret-filter(temp.vwgroup,group==1)$vwret
  diff.vw = as.data.frame(diff.vw) 
  diff.vw = rename(diff.vw,vwret=diff.vw)
  diff.vw$year = filter(temp.group,group==2)$year
  panel <- panel %>%
    merge(.,diff, by="year") %>%
    merge(.,diff.vw, by="year")
  panel$group = i
  panel.all = rbind(panel.all,panel)
}

# create panel for industry concentration 
load("df.RData")
df <- df %>%
  filter(year>=1965 & year<=2018)

df.group <- df %>%
  group_by(year,industry) %>%
  summarise_at(c("sale","at","BE"),sum,na.rm=TRUE) %>%
  ungroup %>%
  rename(industry.sale = sale,
         industry.asset = at,
         industry.equity = BE)
panel.hhi <- df %>%
  merge(.,df.group,by=c("year","industry")) %>%
  mutate(s.sale = (sale/industry.sale)^2,
         s.asset = (at/industry.asset)^2,
         s.equity = (BE/industry.equity)^2) 
panel.hhi <- panel.hhi %>%
  group_by(year,industry) %>%
  summarise_at(c("s.sale","s.asset","s.equity"),sum,na.rm=TRUE) %>%
  ungroup 
panel.hhi <- panel.hhi %>%
  arrange(year,industry) %>%
  group_by(industry) %>%
  mutate(hhi.sale = (s.sale+lag(s.sale,1)+lag(s.sale,2))/3,
         hhi.asset = (s.asset+lag(s.asset,1)+lag(s.asset,2))/3,
         hhi.equity = (s.equity+lag(s.equity,1)+lag(s.equity,2))/3) %>%
  ungroup
panel.hhi <- panel.hhi %>%
  filter(year>=1968) %>%
  filter(!is.na(industry))%>%
  select(industry,year,hhi.asset,hhi.equity,hhi.sale)
panel.all=rename(panel.all,industry=group)
panel = merge(panel.all,panel.hhi,by=c("year","industry"),all.x=TRUE)    

# correlation of HHI
cor = cor(as.matrix(panel.hhi[,c("hhi.asset","hhi.equity","hhi.sale")]),use="pairwise.complete.obs")
write.csv(cor,"table7 - cor.csv")
# run ols regressions
table8 = data.frame(row.names = seq(9))
temp<- panel #full sample
temp <- panel %>%  #1984-1989
  filter(year>=1984&year<=1989)
reg = lm("ew.slope ~ 1 + hhi.asset",data=temp)
table8[1,"ew.slope1"] = reg$coefficients[1]
table8[2,"ew.slope1"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"ew.slope1"] = reg$coefficients[2]
table8[4,"ew.slope1"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"ew.slope1"] = summary(reg)$r.squared
reg = lm("ew.slope ~ 1 + hhi.sale",data=temp)
table8[1,"ew.slope2"] = reg$coefficients[1]
table8[2,"ew.slope2"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[5,"ew.slope2"] = reg$coefficients[2]
table8[6,"ew.slope2"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"ew.slope2"] = summary(reg)$r.squared
reg = lm("ew.slope ~ 1 + hhi.equity",data=temp)
table8[1,"ew.slope3"] = reg$coefficients[1]
table8[2,"ew.slope3"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[7,"ew.slope3"] = reg$coefficients[2]
table8[8,"ew.slope3"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"ew.slope3"] = summary(reg)$r.squared
reg = lm("ew.slope ~ 1 + hhi.asset + hhi.sale + hhi.equity",data=temp)
table8[1,"ew.slope"] = reg$coefficients[1]
table8[2,"ew.slope"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"ew.slope"] = reg$coefficients[2]
table8[4,"ew.slope"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[5,"ew.slope"] = reg$coefficients[3]
table8[6,"ew.slope"] = coeftest(reg, vcov = vcovHC(reg))[3,3]
table8[7,"ew.slope"] = reg$coefficients[4]
table8[8,"ew.slope"] = coeftest(reg, vcov = vcovHC(reg))[4,3]
table8[9,"ew.slope"] = summary(reg)$r.squared

reg = lm("vw.slope ~ 1 + hhi.asset",data=temp)
table8[1,"vw.slope1"] = reg$coefficients[1]
table8[2,"vw.slope1"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"vw.slope1"] = reg$coefficients[2]
table8[4,"vw.slope1"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"vw.slope1"] = summary(reg)$r.squared
reg = lm("vw.slope ~ 1 + hhi.sale",data=temp)
table8[1,"vw.slope2"] = reg$coefficients[1]
table8[2,"vw.slope2"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[5,"vw.slope2"] = reg$coefficients[2]
table8[6,"vw.slope2"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"vw.slope2"] = summary(reg)$r.squared
reg = lm("vw.slope ~ 1 + hhi.equity",data=temp)
table8[1,"vw.slope3"] = reg$coefficients[1]
table8[2,"vw.slope3"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[7,"vw.slope3"] = reg$coefficients[2]
table8[8,"vw.slope3"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"vw.slope3"] = summary(reg)$r.squared
reg = lm("vw.slope ~ 1 + hhi.asset + hhi.sale + hhi.equity",data=temp)
table8[1,"vw.slope"] = reg$coefficients[1]
table8[2,"vw.slope"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"vw.slope"] = reg$coefficients[2]
table8[4,"vw.slope"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[5,"vw.slope"] = reg$coefficients[3]
table8[6,"vw.slope"] = coeftest(reg, vcov = vcovHC(reg))[3,3]
table8[7,"vw.slope"] = reg$coefficients[4]
table8[8,"vw.slope"] = coeftest(reg, vcov = vcovHC(reg))[4,3]
table8[9,"vw.slope"] = summary(reg)$r.squared

reg = lm("ewret ~ 1 + hhi.asset",data=temp)
table8[1,"ewret1"] = reg$coefficients[1]
table8[2,"ewret1"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"ewret1"] = reg$coefficients[2]
table8[4,"ewret1"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"ewret1"] = summary(reg)$r.squared
reg = lm("ewret ~ 1 + hhi.sale",data=temp)
table8[1,"ewret2"] = reg$coefficients[1]
table8[2,"ewret2"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[5,"ewret2"] = reg$coefficients[2]
table8[6,"ewret2"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"ewret2"] = summary(reg)$r.squared
reg = lm("ewret ~ 1 + hhi.equity",data=temp)
table8[1,"ewret3"] = reg$coefficients[1]
table8[2,"ewret3"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[7,"ewret3"] = reg$coefficients[2]
table8[8,"ewret3"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"ewret3"] = summary(reg)$r.squared
reg = lm("ewret ~ 1 + hhi.asset + hhi.sale + hhi.equity",data=temp)
table8[1,"ewret"] = reg$coefficients[1]
table8[2,"ewret"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"ewret"] = reg$coefficients[2]
table8[4,"ewret"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[5,"ewret"] = reg$coefficients[3]
table8[6,"ewret"] = coeftest(reg, vcov = vcovHC(reg))[3,3]
table8[7,"ewret"] = reg$coefficients[4]
table8[8,"ewret"] = coeftest(reg, vcov = vcovHC(reg))[4,3]
table8[9,"ewret"] = summary(reg)$r.squared


reg = lm("vwret ~ 1 + hhi.asset",data=temp)
table8[1,"vwret1"] = reg$coefficients[1]
table8[2,"vwret1"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"vwret1"] = reg$coefficients[2]
table8[4,"vwret1"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"vwret1"] = summary(reg)$r.squared
reg = lm("vwret ~ 1 + hhi.sale",data=temp)
table8[1,"vwret2"] = reg$coefficients[1]
table8[2,"vwret2"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[5,"vwret2"] = reg$coefficients[2]
table8[6,"vwret2"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"vwret2"] = summary(reg)$r.squared
reg = lm("vwret ~ 1 + hhi.equity",data=temp)
table8[1,"vwret3"] = reg$coefficients[1]
table8[2,"vwret3"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[7,"vwret3"] = reg$coefficients[2]
table8[8,"vwret3"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[9,"vwret3"] = summary(reg)$r.squared
reg = lm("vwret ~ 1 + hhi.asset + hhi.sale + hhi.equity",data=temp)
table8[1,"vwret"] = reg$coefficients[1]
table8[2,"vwret"] = coeftest(reg, vcov = vcovHC(reg))[1,3]
table8[3,"vwret"] = reg$coefficients[2]
table8[4,"vwret"] = coeftest(reg, vcov = vcovHC(reg))[2,3]
table8[5,"vwret"] = reg$coefficients[3]
table8[6,"vwret"] = coeftest(reg, vcov = vcovHC(reg))[3,3]
table8[7,"vwret"] = reg$coefficients[4]
table8[8,"vwret"] = coeftest(reg, vcov = vcovHC(reg))[4,3]
table8[9,"vwret"] = summary(reg)$r.squared
write.csv(table8,"table8.csv")
