CREATE OR REPLACE PROCEDURE CDIP_IN.PRC_AAR08_BESTANDSKDN_MSD_MIGR
IS
l_count number;
l_procedure_name VARCHAR2(100);
BEGIN

l_procedure_name := $$PLSQL_UNIT;

-- Reset of previous runs
UPDATE cdip_out.T_GP_FINAL_AZ_2310_V103 
SET
FLAG_BESTANDSKUNDE = 0, 
ZVID = NULL
;
commit;


 MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 final 
USING (
with ivbk as
(
select distinct "BED_ID" "SOURCE_ID", "ZENTRALE_VERSATEL_ID", "BK" "FLAG_BESTANDSKUNDE"
from "CDIP_IN"."T_BED_BESTANDSKUNDEN_IN"
where "BK" = 1
and TIMESTAMP_COL = (select max(timestamp_col) from "CDIP_IN"."T_BED_BESTANDSKUNDEN_IN")
UNION ALL
select distinct "DUNS_NUMMER" "SOURCE_ID", "ZENTRALE_VERSATEL_ID", "BK" "FLAG_BESTANDSKUNDE"
from "CDIP_IN"."T_DNB_BESTANDSKUNDEN_IN"
where "BK" = 1
and TIMESTAMP_COL = (select max(timestamp_col) from "CDIP_IN"."T_DNB_BESTANDSKUNDEN_IN")
),
iv_mm as
(
select mm."DUNS_Nummer" "DUNS_Nummer", mm.bed_id "BED_ID", mm."match_ID" "match_ID", mm."PVID"
from CDIP_OUT.T_GP_RAW_MM_AZ_2310_V103 mm
)
SELECT distinct("PVID"), max("ZENTRALE_VERSATEL_ID") "ZVID" -- Auswahl aller betroffenen PVIDs, welche entweder auf Basis der DNB/BED_ID oder auf einer Rufnummer innerhalb einer Matching Group/PVID
from ivbk iv_bk, iv_mm ivmm
where iv_bk."SOURCE_ID" = ivmm."DUNS_Nummer" OR iv_bk."SOURCE_ID" = ivmm."BED_ID" -- 44528
group by "PVID"
) ivq
ON (final."PVID" = ivq."PVID")
WHEN MATCHED THEN UPDATE SET final."FLAG_BESTANDSKUNDE" = 1,
                             final."ZVID" = ivq."ZVID"
;

l_count := SQL%ROWCOUNT;
COMMIT;

DBMS_OUTPUT.PUT_LINE('Affected Records Count: ' || l_count);

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count: ' || l_count);
COMMIT;

END;