-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Question 1
-- MAGIC Looking at the dossier's number of participants in the dev_sample:

-- COMMAND ----------

select totalDossierParticipants, count(*) as Volume,
        concat(round(100*(count(*) / 29227900),2),' %') as Volume_Perc
from decisionscience.dossier_ptp_development_sample
group by 1
order by 1 asc

-- COMMAND ----------

-- Looking at the most recent month in the dev_sample:
select totalDossierParticipants, count(*) as Volume,
        concat(round(100*(count(*) / 736837),2),' %') as Volume_Perc
from decisionscience.dossier_ptp_development_sample
where ObservationDate = '2024-10-31'
group by 1
order by 1 asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 2
-- MAGIC Looking at the transactions and linking them to participants:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First, let's look at how often we have 1 particpant per account (and therefore we can link transactions perfectly without the need for Dials & Payment Plans):

-- COMMAND ----------

-- Looking at the most recent month in the dev_sample:
select case when totalDossierParticipants + 1 = numAccounts then 1 else 0 end as Flag_1to1Relation, count(*) as Volume,
        concat(round(100*(count(*) / 114805),2),' %') as Volume_Perc
from decisionscience.dossier_ptp_development_sample
where ObservationDate = '2024-10-31'
and totalDossierParticipants >= 1
group by 1
order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Secondly, let's explore how we would link the payments and how easy it is to do:

-- COMMAND ----------

-- DBTITLE 1,Accounts
CREATE OR REPLACE TEMPORARY FUNCTION SQL_MthDATEDIFF(d1 DATE, d2 DATE)
  RETURNS INT
  RETURN CAST(MONTHS_BETWEEN('1900-01-01', d1) AS INT) - CAST(MONTHS_BETWEEN('1900-01-01', d2) AS INT)
;
CREATE OR REPLACE TEMP VIEW purchases_clean AS
SELECT * EXCEPT (PurchaseDate, DeterminationDate),
       CASE WHEN PurchaseName = 'BBVA-Hera-Carval' THEN TO_DATE('2019-12-17')
           ELSE PurchaseDate
       END AS PurchaseDate,
       CASE WHEN PurchaseName = 'BBVA-Hera-Carval' THEN TO_DATE('2019-09-01')
           ELSE DeterminationDate
       END AS DeterminationDate -- DetDate won't be used anyway so it doesn't really matter
FROM silver_spain.amp_purchases
;
CREATE OR REPLACE TEMP VIEW accounts_clean AS
WITH accts_w_flags AS (
  SELECT *,
         CASE WHEN DefaultDate < AccountOpenDate THEN 1
              WHEN AccountEndDate < AccountOpenDate THEN 1
              WHEN AccountEndDate < StartingBalanceDate THEN 1
              WHEN StartingBalanceDate < AccountOpenDate THEN 1
              WHEN YEAR(AccountOpenDate)=9999 THEN 1
              ELSE 0
         END AS DateIssueFlag
  FROM (SELECT * EXCEPT (StartingBalanceDate), CASE WHEN PurchaseID=1412 THEN TO_DATE('2019-12-17') ELSE StartingBalanceDate END AS StartingBalanceDate FROM silver_spain.amp_accounts)
)
SELECT a.AccountID,
       d.dossier_id,
       d.id AS debt_id,
       a.ProductID,
       pr.ProductName,
       CASE WHEN LCASE(ProductName) IN ('auto','car loan','leasing','motorcycle loan','auto direct','auto claim excess','hire purchase') THEN 'Auto'
            WHEN LCASE(ProductName) IN ('loan','loans & credits','syndicated loan','construction loan','loan - point of sale')  THEN 'Loan'
            WHEN LCASE(ProductName) IN ('mortgage shortfall','mortgage','mortgage loan')                                       THEN 'MT'
            WHEN LCASE(ProductName) IN ('overdraft','current account','savings account')                                       THEN 'Overdraft'
            WHEN LCASE(ProductName) IN ('credits','credit account','credit line','revolving credit agreement')                 THEN 'Credit'
            WHEN LCASE(ProductName) IN ('mail order','mail order and retail')                                                  THEN 'MailOrder'
            WHEN LCASE(COALESCE(ProductName,'null')) IN ('null','insurance','discounted bill','guarantee','commercial paper','com. paper','derivative',
                                                         'import/export facilities','confirming','executed guarantees','renting','aged') THEN 'Other'
            WHEN LCASE(ProductName) IN ('bk','iva','td','hmb','arg') THEN ProductName
            ELSE REPLACE(INITCAP(ProductName)," ","")
       END AS ProductGroup,                                                         
       pr.EquifaxFinanceCode,
       a.PurchaseID,
       a.OriginatorID,
       a.OriginationChannel,
       DATE(p.DeterminationDate) AS DeterminationDate,
       DATE(p.PurchaseDate) AS PurchaseDate,
       DATE(a.DefaultDate) AS DefaultDate,
       DATE(a.WriteOffDate) AS WriteOffDate,
       DATE(a.AccountOpenDate) AS AccountOpenDate,
       DATE(a.AccountEndDate) AS AccountEndDate,
       SQL_MthDATEDIFF(DATE(p.PurchaseDate), DATE(a.AccountEndDate)) AS CloseMOB,
       UCASE(a.ClosureReasonID) AS ClosureReasonID,
       INT(a.IsActive) AS IsActive,
       CASE WHEN UCASE(a.ClosureReasonID) IN ('PIF','SET') THEN 0
            WHEN UCASE(a.ClosureReasonID) IN ('PBK','BBK') THEN 1
            ELSE INT(a.IsPutback) 
       END AS IsPutback,
       CASE WHEN UCASE(a.ClosureReasonID) IN ('PIF','SET') THEN NULL
            WHEN UCASE(a.ClosureReasonID) IN ('PBK','BBK') THEN DATE(a.AccountEndDate)
            ELSE DATE(a.PutbackDate)
       END AS PutbackDate,
       a.Region,
       a.PortfolioDescription,
       DATE(a.dateCreated) AS dateCreated,
       DATE(a.StartingBalanceDate) AS StartingBalanceDate,
       a.TotalStartingBalance,
       DATE(a.ModifiedOn) AS ModifiedOn,
       COALESCE(INT(a.IsLitigatedAtPurchase),0) AS IsLitigatedAtPurchase,
       a.SOEAccountID,
       a.EntityId,
       COALESCE(INT(a.IsLitigated),0) AS IsLitigated,
       a.DateIssueFlag
FROM accts_w_flags a
LEFT JOIN purchases_clean p ON p.PurchaseID=a.PurchaseID
INNER JOIN silver_spain.bronze_spain_amp_sor_placementaccountreference par ON par.AccountID=a.AccountID -- table clean already
INNER JOIN silver_spain.bronze_spain_temis_dbo_debt d ON d.customer_debt_id=par.GroveAccountReference   -- table clean already
INNER JOIN silver_spain.temis_dossier dos ON d.dossier_id=dos.id
LEFT JOIN silver_spain.temis_customer cus ON dos.customer_id=cus.id
LEFT JOIN silver_spain.bronze_spain_amp_sor_products pr ON a.ProductID=pr.ProductID
WHERE a.Region='Spain'   --Reduce to only Spain Accounts
  AND a.DateIssueFlag=0  --Exclude date issues
  AND cus.customer_type_id=8 --Exclude Non Own Debt
;

-- COMMAND ----------

-- DBTITLE 1,Participants
CREATE OR REPLACE TEMP VIEW temis_debt_participant_step1 AS
SELECT * EXCEPT(cust_participant_order), 
       CASE WHEN cust_participant_order ='' THEN NULL ELSE cust_participant_order END AS cust_participant_order 
FROM silver_spain.bronze_spain_temis_dbo_debt_participant
;

CREATE OR REPLACE TEMP VIEW temis_debt_participant_clean AS
SELECT *,
       CASE WHEN numParticipants=1 THEN 1
            ELSE ROW_NUMBER() OVER (PARTITION BY debt_id ORDER BY CASE WHEN LCASE(name) RLIKE 'holder.*|co.*' THEN 1
                                                                       WHEN LCASE(name) RLIKE 'guar.*' THEN 2
                                                                       ELSE 3 END ASC, COALESCE(cust_participant_order,999) ASC, id ASC)
       END AS prtpnt_order
FROM (
SELECT dp.* EXCEPT (dp.cust_language, dp.is_valid_identity, dp.valid_identity_date, dp.bureau_date, dp.debt_participant_state_type_id, dp.exonerated_by_user_sgi_id, dp.solvency, dp.xml_data, dp.cust_birth_date),
       CASE WHEN YEAR(dp.cust_birth_date) <=1920 OR YEAR(dp.cust_birth_date) >= 2026 THEN NULL
            ELSE DATE(dp.cust_birth_date)
       END AS cust_birth_date,
       CASE WHEN lower(cpr.name) IN ('otro') THEN 'Other'
            WHEN lower(cpr.name) IN ('autorizado') THEN 'Authorised'
            WHEN lower(cpr.name) IN ('co-holder','coholder','cotitular','coborrower1') THEN 'Co-holder'
            ELSE INITCAP(regexp_replace(regexp_replace(lower(cpr.name),r"(titular|borrower)(\d*)|tit\z|hol\z|debtor\z|main debtor\z","Holder\$2"),r'(fiador)(\d*)|fia|gua\z|avalista','Guarantor\$2')) 
       END AS name,
       cpr.description,
       COUNT(dp.id) OVER (PARTITION BY dp.debt_id) AS numParticipants
FROM temis_debt_participant_step1 dp
INNER JOIN silver_spain.bronze_spain_temis_dbo_customer_participant_role cpr ON dp.customer_participant_role_id=cpr.id
-- WHERE dp.deleted != True 
)

-- COMMAND ----------

-- DBTITLE 1,CustomerAccounts
CREATE OR REPLACE TEMP VIEW custaccounts_clean AS
SELECT CustomerAccountID, 
       AccountID,
       CustomerID,
       UCASE(CustomerTypeID) AS CustomerTypeID,
       UCASE(ParticipantTypeId) AS ParticipantTypeId,
       ParticipantRank,
       INT(IsExonerated) AS IsExonerated,
       DATE(ExoneratedOn) AS ExoneratedOn
FROM silver_spain.amp_customeraccount

-- COMMAND ----------

-- DBTITLE 1,Dialler
CREATE OR REPLACE TEMP VIEW dialler_inbound_clean AS
SELECT INT(REPLACE(TRIM(filler2),".","")) AS dossier_id, -- some dossiers have a trailing "." (removed by replace), some dossiers have a leading "0" (removed by INT), some dossiers have leading/trailing whitespace (removed by trim)
       TRIM(filler4) AS person_id,
       DATE(call_date) AS call_date,
       TIMESTAMP(call_time) AS call_time,
       ani_acode,
      --  ani_phone,
       dnis_phone,
       UCASE(TRIM(status)) AS status,
       status_description,
       UCASE(TRIM(addi_status)) AS addi_status,
       addi_status_description,
       call_type,
       CASE WHEN call_type=0 THEN 'Inbound Call'
            WHEN call_type=1 THEN 'Group-Group Transfer'
            WHEN call_type=2 THEN 'External Transfer'
            WHEN call_type=3 THEN 'Agent-Agent Transfer'
            WHEN call_type=4 THEN 'Switched Application'
            WHEN call_type=5 THEN 'ACD Transfer Successful'
            WHEN call_type=6 THEN 'ACD Transfer Failed or Cancelled'
            ELSE 'ERROR'
       END AS inbound_call_type,
       CASE WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) IN ('RPCB','RPE','RPEN','RPLO','RPNG','RPY8','TPCI','TPN','TPX','RPD')
                 AND LCASE(TRIM(addi_status_description)) = 'acuerdo de pago' THEN 0
            WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) = 'RPYH' AND LCASE(TRIM(addi_status_description)) IN ('empresa en concurso', 'empresa en concuso') THEN 0
            WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) = 'RPYM' AND LCASE(TRIM(addi_status_description)) = 'menor de edad' THEN 0
            WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) IN ('RPYE', 'RPYS', 'HPHP') THEN 0
            ELSE 1
       END AS include_calls
FROM silver_spain.noble_dialler_inbound
WHERE ani_country_id=34
  AND UCASE(TRIM(status)) NOT IN ('T','RS','')
;
CREATE OR REPLACE TEMP VIEW dialler_outbound_clean AS
SELECT INT(REPLACE(TRIM(lm_filler2),".","")) AS dossier_id, -- some dossiers have a trailing "." (removed by replace), some dossiers have a leading "0" (removed by INT), some dossiers have leading/trailing whitespace (removed by trim)
       TRIM(lm_filler4) AS person_id,
       DATE(act_date) AS act_date,
       TIMESTAMP(act_time) AS act_time,
       caller_ani,
       phone_type,
       phone_descr,
       UCASE(TRIM(status)) AS status,
       status_description,
       UCASE(TRIM(addi_status)) AS addi_status,
       addi_status_description,
       call_type,
       CASE WHEN call_type=0 THEN 'Outbound Call'
            WHEN call_type=1 THEN 'Outbound Call Fail'
            WHEN call_type=2 THEN 'Transfer Success'
            WHEN call_type=3 THEN 'Transfer Failed'
            WHEN call_type=4 THEN 'Callback Success'
            WHEN call_type=5 THEN 'Callback Failed'
            WHEN call_type=6 THEN 'Outbound Call Dropped'
            WHEN call_type=7 THEN 'Sent To IVR'
            WHEN call_type=8 THEN 'ACD Transfer Successful'
            WHEN call_type=9 THEN 'ACD Transfer Failed or Cancelled'
            ELSE 'ERROR'
       END AS outbound_call_type,
       CASE WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) IN ('RPCB','RPE','RPEN','RPLO','RPNG','RPY8','TPCI','TPN','TPX','RPD')
                 AND LCASE(TRIM(addi_status_description)) = 'acuerdo de pago' THEN 0
            WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) = 'RPYH' AND LCASE(TRIM(addi_status_description)) IN ('empresa en concurso', 'empresa en concuso') THEN 0
            WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) = 'RPYM' AND LCASE(TRIM(addi_status_description)) = 'menor de edad' THEN 0
            WHEN CONCAT(UCASE(TRIM(status)), UCASE(TRIM(addi_status))) IN ('RPYE', 'RPYS', 'HPHP') THEN 0
            ELSE 1
       END AS include_calls
FROM silver_spain.noble_dialler_outbound
WHERE country_id=34
  AND UCASE(TRIM(status)) NOT IN ('T','RS','')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.pandas as ps
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC date_df = spark.createDataFrame(ps.date_range('2021-03-31',periods=46, freq='m').strftime('%Y-%m-%d').to_numpy(), schema="Month_str STRING")\
-- MAGIC                 .withColumn('ObservationDate', F.col('Month_str').cast('DATE'))
-- MAGIC
-- MAGIC date_df.createOrReplaceTempView('date_df')

-- COMMAND ----------

select * from date_df

-- COMMAND ----------

-- DBTITLE 1,Transactions
-- CLEAN RAW TRANSACTIONS
CREATE OR REPLACE TEMP VIEW transactions_clean AS
SELECT t.AccountID, 
       DATE(t.TransactionDate) AS TransactionDate,
       LAST_DAY(DATE(t.TransactionDate)) AS TransMth,
       CASE WHEN SIGN(SUM(t.GrossTransaction)) = 1 THEN 'PAY'
            WHEN SIGN(SUM(t.GrossTransaction)) = -1 THEN 'PRV'
            ELSE ''
       END AS TransactionTypeID,
       SUM(CASE WHEN UCASE(t.TransactionTypeID) IN ('PAY','PRV','XRE') THEN t.GrossTransaction ELSE 0 END) AS allCash,
       SUM(CASE WHEN UCASE(t.TransactionTypeID) IN ('PAY','PRV','XRE') AND UPPER(t.PaymentMethodID) IN ('CON','MAN') THEN t.GrossTransaction ELSE 0 END) AS legalCash,
       SUM(CASE WHEN UCASE(t.TransactionTypeID) IN ('PAY','PRV','XRE') AND UPPER(t.PaymentMethodID) NOT IN ('CON','MAN') THEN t.GrossTransaction ELSE 0 END) AS amicableCash,
       SUM(t.GrossTransaction) AS balAdjAmount
FROM silver_spain.amp_transactions t
INNER JOIN accounts_clean a ON a.AccountID = t.AccountID
WHERE UCASE(t.CurrencyCode) IN ('EUR')      -- Only include EUR transactions
  AND t.TransactionDate IS NOT NULL         -- Exclude transactions with no date
  AND (t.PreDeterminationFlag = False OR (a.PurchaseID=1412 AND t.TransactionDate>=a.PurchaseDate)) -- Exclude pre-purchase transactions (adjusted for Hera Carval)
  AND t.GrossTransaction != 0               -- Exclude zero transactions
  -- Exclude accounts whose net transactions equate to 0 or less -- this cleaning step is not required for PTP model since the outcome window does not consider the entire lifetime of the account, instead it focuses on just a few months post observation.
 AND t.TransactionDate <= '2024-12-31'
GROUP BY t.AccountID, t.TransactionDate
;
-- ARTICIFICIALLY ADD A ROW FOR EACH MONTH BUT WITH ZERO CASH TO FORCE IT TO CALCULATE A CUMULATIVE FOR EACH MONTH
CREATE OR REPLACE TEMP VIEW artificial_trans_mth AS
SELECT a.AccountID,
       dt.ObservationDate AS TransactionDate,
       dt.ObservationDate AS TransMth,
       '' AS TransactionTypeID,
       0 AS allCash,
       0 AS legalCash,
       0 AS amicableCash,
       0 AS balAdjAmount
FROM (SELECT AccountID FROM transactions_clean GROUP BY 1) a
CROSS JOIN date_df dt
;
CREATE OR REPLACE TEMP VIEW transactions_clean_artificial AS
SELECT *
FROM transactions_clean
UNION ALL
SELECT *
FROM artificial_trans_mth
;
-- CREATE MONTHLY VIEW OF TRANSACTIONS
CREATE OR REPLACE TEMP VIEW transactions_monthly AS
SELECT *,
       SUM(allCash) OVER (PARTITION BY AccountID ORDER BY TransMth) AS cuml_allCash,
       SUM(amicableCash) OVER (PARTITION BY AccountID ORDER BY TransMth) AS cuml_amicableCash,
       SUM(legalCash) OVER (PARTITION BY AccountID ORDER BY TransMth) AS cuml_legalCash,
       SUM(balAdjAmount) OVER (PARTITION BY AccountID ORDER BY TransMth) AS cuml_balAdjAmount
FROM (SELECT AccountID,
             TransMth,
             SUM(allCash) AS allCash,
             SUM(legalCash) AS legalCash,
             SUM(amicableCash) AS amicableCash,
             SUM(balAdjAmount) AS balAdjAmount
      FROM transactions_clean_artificial
      GROUP BY 1,2)

-- COMMAND ----------

select * 
from decisionscience.dossier_ptp_development_sample
where numAccounts = 1
and totalDossierParticipants = 0
and cumlCash > 0 limit 5

-- COMMAND ----------

select * 
from decisionscience.dossier_ptp_development_sample
where dossier_id = 7016954

-- COMMAND ----------

select * from accounts_clean where dossier_id = 7016954

-- COMMAND ----------

select * from transactions_clean 
where AccountID = 36106339 
order by TransactionDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Example 2, where we would need to link the transactions to specific participants:

-- COMMAND ----------

select * 
from decisionscience.dossier_ptp_development_sample
where totalDossierParticipants = 1
and numAccounts = 1
and allCash_9m_sum > 0 limit 5

-- COMMAND ----------

select * 
from decisionscience.dossier_ptp_development_sample
where dossier_id = 10383148

-- COMMAND ----------

select * from accounts_clean where dossier_id = 10383148

-- COMMAND ----------

select * from transactions_clean 
where AccountID = 36369975 
order by TransactionDate

-- COMMAND ----------

select * from silver_spain.amp_customeraccount where AccountID = 36369975

-- COMMAND ----------

select *,
CASE WHEN status='RP' AND include_calls=1 AND addi_status in ('GH','GP','HA','HR','PW','Z1','Z2','Z5','GC','HP','YS','YE') THEN 1 ELSE 0 END AS InbndPromiseRPC
from dialler_inbound_clean 
where dossier_id = 10383148
order by call_date asc

-- COMMAND ----------

select *
from silver_spain.noble_dialler_inbound
where INT(REPLACE(TRIM(filler2),".","")) = 10383148
order by call_date

-- COMMAND ----------

