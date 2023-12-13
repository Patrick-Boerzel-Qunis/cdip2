CREATE OR REPLACE PROCEDURE CDIP_IN.PRC_AAR13_trassenabst_kat_MSD_MIGR
IS
l_count number;
l_procedure_name VARCHAR2(100);
BEGIN

l_procedure_name := $$PLSQL_UNIT;

-- Reset previous runs
UPDATE CDIP_OUT.T_GP_FINAL_AZ_2310_V103
SET TRASSENABSTANDSKATEGORIE   = '';
commit;

    MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 target
USING (
 select final.pvid,
  CASE 
           WHEN final.ab_trassen_own_ohne_bt = 0 THEN 'OnNet'

           WHEN (final.flag_gueltig = 1 AND (final.ab_trassen_own_ohne_bt BETWEEN 0 AND 15) AND final.flag_strassenseite = 0) THEN 'Buildings Passed - falsche Strassenseite'
           WHEN (final.flag_gueltig = 1 AND (final.ab_trassen_own_ohne_bt BETWEEN 0 AND 15) AND final.flag_strassenseite = 1) THEN 'Buildings Passed - richtige Strassenseite'

           WHEN (final.flag_gueltig = 1 AND final.ab_trassen_own_ohne_bt > 15 AND final.ab_plantrassen_5g <= 15 AND final.flag_strassenseite = 0) THEN 'Buildings Passed geplant 5G- falsche Strassenseite'
           WHEN (final.flag_gueltig = 1 AND final.ab_trassen_own_ohne_bt > 15 AND final.ab_plantrassen_5g <= 15 AND final.flag_strassenseite = 1) THEN 'Buildings Passed geplant 5G- richtige Strassenseite'

           WHEN (final.flag_gueltig = 1 AND final.ab_trassen_own_ohne_bt > 15 AND final.ab_plantrassen_5g > 15 AND (final.ab_plantrassen_normal <= 15 OR final.ab_plantrassen_nicht_webom <= 15) AND final.flag_strassenseite = 0) THEN 'Buildings Passed geplant - falsche Strassenseite'
           WHEN (final.flag_gueltig = 1 AND final.ab_trassen_own_ohne_bt > 15 AND final.ab_plantrassen_5g > 15 AND (final.ab_plantrassen_normal <= 15 OR final.ab_plantrassen_nicht_webom <= 15) AND final.flag_strassenseite = 1) THEN 'Buildings Passed geplant - richtige Strassenseite'

           WHEN (final.flag_gueltig = 1 AND (final.ab_trassen_own_ohne_bt BETWEEN 15 AND 50) AND final.ab_plantrassen_5g > 15 AND final.ab_plantrassen_normal > 15 AND final.ab_plantrassen_nicht_webom > 15 ) THEN 'NearNet 50'

           WHEN (final.flag_gueltig = 1 AND (final.ab_trassen_own_ohne_bt > 50) AND final.ab_plantrassen_5g > 15 AND final.ab_plantrassen_normal > 15 AND final.ab_plantrassen_nicht_webom > 15 ) THEN 'FarNet'

           WHEN (final.flag_gueltig <> 0 OR nvl(final.flag_gueltig,99) = 99) THEN 'FarNet'

           ELSE 'FarNet'
       END AS TRASSENABSTANDSKATEGORIE
        FROM CDIP_OUT.T_GP_FINAL_AZ_2310_V103 final
        ) ivtr 
ON (target.pvid   =   ivtr.pvid)
WHEN MATCHED THEN
    UPDATE SET target.TRASSENABSTANDSKATEGORIE   =   ivtr.TRASSENABSTANDSKATEGORIE
        ;

l_count := SQL%ROWCOUNT;
COMMIT;

DBMS_OUTPUT.PUT_LINE('Affected Records Count: ' || l_count);

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count: ' || l_count);
COMMIT;

END; 
