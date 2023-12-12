CREATE OR REPLACE PROCEDURE CDIP_IN.PRC_AAR06_BLACKLIST_MSD_MIGR
IS
l_count number;
l_procedure_name VARCHAR2(100);
BEGIN

l_procedure_name := $$PLSQL_UNIT;

-- Reset previous runs
UPDATE CDIP_OUT.T_GP_FINAL_AZ_2310_V103
SET "FLAG_BLACKLIST" = 0
where nvl("FLAG_BLACKLIST",0) = 1 OR nvl("FLAG_BLACKLIST",0) = 0
;
commit;


MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 verfl
USING (
WITH iv_blacklist AS (
	SELECT ID BL_ID, to_number(substr(ID,5)) "SOURCE_ID", regexp_replace(RUFNUMMER, '\s', '') RUFNUMMER,GUELTIG_BIS

FROM CDIP_IN.V_BLACKLIST
	WHERE GUELTIG_BIS is null
    and NOT REGEXP_LIKE( substr(ID,5), '[^0-9]+')
), 

iv_mm as
(
select mm."DUNS_Nummer" "DUNS_Nummer", mm.bed_id "BED_ID", mm."match_ID" "match_ID", mm."PVID", decode(mm."Telefon",NULL,NULL,'+49' || ltrim(mm."Vorwahl_Telefon",'0') || mm."Telefon") "TELEFON"
from CDIP_OUT.T_GP_RAW_MM_AZ_2310_V103 mm
)

SELECT distinct("PVID")  -- Auswahl aller betroffenen PVIDs, welche entweder auf Basis der DNB/BED_ID oder auf einer Rufnummer innerhalb einer Matching Group/PVID
from iv_blacklist ivb, iv_mm ivmm
where ivb."SOURCE_ID" = ivmm."DUNS_Nummer" OR ivb."SOURCE_ID" = ivmm."BED_ID"
OR ivb.rufnummer = ivmm."TELEFON"
) ivq
ON (verfl."PVID" = ivq."PVID")
-- 65733 unterschiedliche PVIDs
WHEN MATCHED THEN UPDATE SET verfl."FLAG_BLACKLIST" = 1
;
l_count := SQL%ROWCOUNT;
COMMIT;

DBMS_OUTPUT.PUT_LINE('Affected Records Count: ' || l_count);

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count: ' || l_count);
COMMIT;

END;